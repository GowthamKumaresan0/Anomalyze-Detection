import os, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

# ========= config =========
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "processed-log")   # Drain3 output here
OUTPUT_TOPIC= os.getenv("OUTPUT_TOPIC", "anomalies")
CHECKPOINT  = os.getenv("CHECKPOINT", "file:///tmp/checkpoints/logbert_v2")
SEQ_LEN     = int(os.getenv("SEQ_LEN", "20"))
THRESH      = float(os.getenv("THRESH", "0.8"))
MODEL_PATH  = os.getenv("LOGBERT_MODEL", "").strip()      # set to your .pt/.pth

# ========= spark session =========
spark = (SparkSession.builder
         .appName("LogBERT-StructuredStreaming")
         .config("spark.sql.session.timeZone", "Asia/Kolkata")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ========= input schema coming from Drain3 bridge =========
schema = (StructType()
          .add("raw_log", StringType())
          .add("template", StringType())
          .add("template_id", IntegerType())
          .add("ts", StringType()))  # ISO-ish string

# ========= read from Kafka (processed-log) =========
src = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", INPUT_TOPIC)
       .option("startingOffsets", "earliest")   # for first run / testing
       .option("failOnDataLoss", "false")       # don't crash if topic was recreated
       .load())

parsed = (src.selectExpr("CAST(value AS STRING) AS json_str")
          .withColumn("data", from_json(col("json_str"), schema))
          .select(
              to_timestamp(col("data.ts")).alias("ts"),
              col("data.raw_log").alias("raw_log"),
              col("data.template").alias("template"),
              col("data.template_id").alias("template_id")
          ))

# ========= LogBERT runner (lazy-loaded per executor) =========
_runner = None
def get_runner():
    """
    Try to load a real LogBERT model. If MODEL_PATH is missing, fall back to a
    deterministic dummy scorer so the pipeline still runs and you can see output.
    """
    global _runner
    if _runner is not None:
        return _runner

    try:
        import torch

        class Runner:
            def __init__(self, path, device="cpu"):
                self.device = torch.device(device)
                if not os.path.exists(path):
                    raise FileNotFoundError(path)

                # support both TorchScript and eager checkpoints
                try:
                    self.model = torch.jit.load(path, map_location=self.device)
                except Exception:
                    self.model = torch.load(path, map_location=self.device)
                self.model.eval()

            @torch.no_grad()
            def score(self, seqs, pad_id=0):
                import torch
                import torch.nn.functional as F
                input_ids = torch.tensor(seqs, dtype=torch.long, device=self.device)
                attention_mask = (input_ids != pad_id).long()
                out = self.model(input_ids=input_ids, attention_mask=attention_mask) \
                    if hasattr(self.model, "forward") else self.model(input_ids, attention_mask)
                logits = getattr(out, "logits", out)
                logits = logits.float()
                if logits.dim() == 1:      # [B]
                    probs = torch.sigmoid(logits)
                elif logits.size(-1) == 1: # [B,1]
                    probs = torch.sigmoid(logits.squeeze(-1))
                elif logits.size(-1) == 2: # [B,2]
                    probs = F.softmax(logits, dim=-1)[..., 1]
                else:                      # fallback
                    probs = torch.sigmoid(logits.squeeze(-1))
                return probs.detach().clamp(0, 1).cpu().tolist()

        _runner = Runner(MODEL_PATH or "")
        print(f"[LogBERT] Loaded model: {MODEL_PATH}")
        return _runner

    except Exception as e:
        # Dummy scorer for connectivity testing
        print(f"[LogBERT] WARNING: using dummy scorer ({e}). "
              f"Set LOGBERT_MODEL to a .pt/.pth to enable real scoring.")
        import random
        random.seed(42)

        class DummyRunner:
            def score(self, seqs, pad_id=0):
                # stable pseudo-score from the sequence content
                return [min(1.0, (sum(s) % 97) / 100.0) for s in seqs]

        _runner = DummyRunner()
        return _runner

# ========= per-batch logic =========
def foreach_batch(df, batch_id: int):
    cnt = df.count()
    print(f"[foreachBatch] id={batch_id} rows={cnt}")
    if cnt == 0:
        return

    rows = (df.orderBy("ts")
              .select("ts", "raw_log", "template", "template_id")
              .collect())

    PAD_ID = 0
    tids  = [max(0, int(r["template_id"])) + 1 for r in rows]  # shift so 0 is PAD
    raws  = [r["raw_log"] for r in rows]
    temps = [r["template"] for r in rows]
    times = [r["ts"].strftime("%Y-%m-%d %H:%M:%S") if r["ts"] else None for r in rows]

    # sliding window -> sequences of SEQ_LEN
    seqs, metas = [], []
    n = len(tids)
    if n < SEQ_LEN:
        padded = [PAD_ID] * (SEQ_LEN - n) + tids
        seqs.append(padded[-SEQ_LEN:])
        metas.append({"timestamp": times[-1], "raw_log": raws[-1], "template": temps[-1]})
    else:
        for i in range(n - SEQ_LEN + 1):
            seqs.append(tids[i:i + SEQ_LEN])
            metas.append({
                "timestamp": times[i + SEQ_LEN - 1],
                "raw_log": raws[i + SEQ_LEN - 1],
                "template": temps[i + SEQ_LEN - 1]
            })

    runner = get_runner()
    scores = runner.score(seqs, pad_id=PAD_ID)

    out_rows = []
    for meta, s in zip(metas, scores):
        out_rows.append((json.dumps({
            **meta,
            "score": float(s),
            "is_anomaly": bool(s >= THRESH),
            "seq_len": SEQ_LEN
        }),))

    out_df = spark.createDataFrame(out_rows, ["value"])
    (out_df.write
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("topic", OUTPUT_TOPIC)
          .save())

query = (parsed.writeStream
         .foreachBatch(foreach_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime="5 seconds")
         .start())

query.awaitTermination()

