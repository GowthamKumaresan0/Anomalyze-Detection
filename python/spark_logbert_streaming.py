# logbert_stream.py
# Run with Spark 4.x and the Kafka package for your Scala version:
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0
# Env vars:
#   KAFKA_BOOTSTRAP, INPUT_TOPIC, OUTPUT_TOPIC, CHECKPOINT, SEQ_LEN, THRESH,
#   LOGBERT_MODEL, DEBUG, POSITIVE_IS_ANOMALY (1/0), THRESH_COMPARE (>= or >)

import os, json
from collections import defaultdict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, coalesce, current_timestamp, lit
)
from pyspark.sql.types import StructType, StringType, IntegerType

# ========= config =========
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "processed-log")
OUTPUT_TOPIC= os.getenv("OUTPUT_TOPIC", "anomalies")
CHECKPOINT  = os.getenv("CHECKPOINT", "file:///tmp/checkpoints/logbert_v2")
SEQ_LEN     = int(os.getenv("SEQ_LEN", "20"))
THRESH      = float(os.getenv("THRESH", "0.5"))
print(f"[LogBERT] Using THRESH={THRESH}")

MODEL_PATH  = (os.getenv("LOGBERT_MODEL", "") or "").strip()
DEBUG       = os.getenv("DEBUG", "0") not in ("", "0", "false", "False", "FALSE")
POSITIVE_IS_ANOMALY = os.getenv("POSITIVE_IS_ANOMALY", "1") not in ("0","false","False","FALSE","")
THRESH_COMPARE = os.getenv("THRESH_COMPARE", ">=")  # ">=" or ">"

# ========= Spark session =========
spark = (
    SparkSession.builder
    .appName("LogBERT-StructuredStreaming")
    .config("spark.sql.session.timeZone", "Asia/Kolkata")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"[LogBERT] Config -> SEQ_LEN={SEQ_LEN} THRESH={THRESH} "
      f"POSITIVE_IS_ANOMALY={POSITIVE_IS_ANOMALY} THRESH_COMPARE='{THRESH_COMPARE}'")

# ========= input schema coming from Drain3 bridge =========
schema = (
    StructType()
    .add("raw_log", StringType())
    .add("template", StringType())
    .add("template_id", IntegerType())
    .add("ts", StringType())
    .add("source_key", StringType())   # optional
)

# ========= read from Kafka =========
src = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Keep source_key and make ts non-null to maintain ordering stability
parsed = (
    src.selectExpr("CAST(value AS STRING) AS json_str")
       .withColumn("data", from_json(col("json_str"), schema))
       .select(
           coalesce(to_timestamp(col("data.ts")), current_timestamp()).alias("ts"),
           col("data.raw_log").alias("raw_log"),
           col("data.template").alias("template"),
           col("data.template_id").alias("template_id"),
           coalesce(col("data.source_key"), lit("default")).alias("source_key")
       )
)

# ========= LogBERT runner =========
_runner = None
def get_runner():
    global _runner
    if _runner is not None:
        return _runner

    try:
        import torch

        class Runner:
            def __init__(self, path, device="cpu"):
                self.device = torch.device(device)
                if not path or not os.path.exists(path):
                    raise FileNotFoundError(f"LogBERT model not found: {path}")

                try:
                    self.model = torch.jit.load(path, map_location=self.device)
                    print(f"[LogBERT] Loaded TorchScript model: {path}")
                except Exception:
                    self.model = torch.load(path, map_location=self.device)
                    print(f"[LogBERT] Loaded eager PyTorch model: {path}")

                self.model.eval()

            @torch.no_grad()
            def score(self, seqs, pad_id=0):
                import torch
                input_ids = torch.tensor(seqs, dtype=torch.long, device=self.device)
                attention_mask = (input_ids != pad_id).long()
                try:
                    out = self.model(input_ids=input_ids, attention_mask=attention_mask)
                except TypeError:
                    out = self.model(input_ids, attention_mask)
                logits = getattr(out, "logits", out).float()

                if logits.dim() == 1:          # [B]
                    probs = torch.sigmoid(logits)
                elif logits.size(-1) == 1:     # [B,1]
                    probs = torch.sigmoid(logits.squeeze(-1))
                elif logits.size(-1) == 2:     # [B,2]
                    probs = torch.softmax(logits, dim=-1)[..., 1]
                else:
                    probs = torch.sigmoid(logits.squeeze(-1))
                return probs.detach().clamp(0, 1).cpu().tolist()

        _runner = Runner(MODEL_PATH or "")
        return _runner

    except Exception as e:
        print(f"[LogBERT] WARNING: using dummy scorer ({e})")
        class DummyRunner:
            def score(self, seqs, pad_id=0):
                return [min(1.0, (sum(s) % 97) / 100.0) for s in seqs]
        _runner = DummyRunner()
        return _runner

# ========= cross-batch tails (per source) to avoid PAD-heavy cold starts =========
TAILS = {}  # dict[str, list[int]]

def _shift_token_id(tid):
    """Map Drain3 template_id -> model token id: negatives/None -> PAD(0), else +1."""
    if tid is None:
        return 0
    try:
        v = int(tid)
    except Exception:
        return 0
    return (v + 1) if v >= 0 else 0

def _flag_anomaly(prob):
    """Compute anomaly flag based on config."""
    anomaly_prob = prob if POSITIVE_IS_ANOMALY else (1.0 - prob)
    if THRESH_COMPARE == ">":
        return anomaly_prob > THRESH, anomaly_prob
    else:
        return anomaly_prob >= THRESH, anomaly_prob

# ========= per-batch logic =========
def foreach_batch(df, batch_id: int):
    cnt = df.count()
    print(f"[foreachBatch] id={batch_id} rows={cnt}")
    if cnt == 0:
        return
    df = df.cache()
    rows = (
        df.orderBy("ts")
          .select("source_key", "ts", "raw_log", "template", "template_id")
          .collect()
    )
    #df.unpersist(blocking=False)
    by_key = defaultdict(list)
    for r in rows:
        by_key[r["source_key"]].append(r)

    PAD_ID = 0
    out_rows = []
    runner = get_runner()

    for key, rlist in by_key.items():
        tids  = [_shift_token_id(r["template_id"]) for r in rlist]
        raws  = [r["raw_log"] for r in rlist]
        temps = [r["template"] for r in rlist]
        times = [r["ts"].strftime("%Y-%m-%d %H:%M:%S") if r["ts"] else None for r in rlist]

        prefix = TAILS.get(key, [])
        series = prefix + tids
        seqs, metas = [], []

        if len(series) < SEQ_LEN:
            padded = [PAD_ID] * (SEQ_LEN - len(series)) + series
            seqs.append(padded[-SEQ_LEN:])
            metas.append({
                "source_key": key,
                "timestamp": times[-1],
                "raw_log": raws[-1],
                "template": temps[-1],
            })
        else:
            start_end = max(len(prefix), SEQ_LEN - 1)
            for end_idx in range(start_end, len(series)):
                i = end_idx - SEQ_LEN + 1
                if i < 0:
                    continue
                seqs.append(series[i:end_idx + 1])
                batch_idx = end_idx - len(prefix)
                if 0 <= batch_idx < len(rlist):
                    metas.append({
                        "source_key": key,
                        "timestamp": times[batch_idx],
                        "raw_log": raws[batch_idx],
                        "template": temps[batch_idx],
                    })

        if not seqs:
            continue

        scores = runner.score(seqs, pad_id=PAD_ID)
        TAILS[key] = series[-(SEQ_LEN - 1):] if len(series) >= 1 else prefix

        for meta, s in zip(metas, scores):
            is_anom, anomaly_prob = _flag_anomaly(s)
            out_rows.append((
                json.dumps({
                    **meta,
                    "score": float(s),               # raw model prob (positive class)
                    "anomaly_prob": float(anomaly_prob),
                    "threshold": THRESH,
                    "is_anomaly": bool(is_anom),
                    "seq_len": SEQ_LEN
                }),
            ))

        if DEBUG and seqs:
            try:
                print(f"[DEBUG] key={key} windows={len(seqs)} "
                      f"first_seq={seqs[0][:min(10, SEQ_LEN)]} "
                      f"first_score={scores[0]:.4f} "
                      f"anomaly_prob={_flag_anomaly(scores[0])[1]:.4f} "
                      f"thresh={THRESH} compare='{THRESH_COMPARE}' "
                      f"pos_is_anom={POSITIVE_IS_ANOMALY}")
            except Exception:
                pass

    if not out_rows:
        if DEBUG:
            print("[DEBUG] No output rows this batch.")
        return

    out_df = spark.createDataFrame(out_rows, ["value"])
    (
        out_df.write
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("topic", OUTPUT_TOPIC)
             .save()
    )

# ========= start streaming =========
query = (
    parsed.writeStream
          .foreachBatch(foreach_batch)
          .option("checkpointLocation", CHECKPOINT)
          .trigger(processingTime="5 seconds")
          .start()
)

query.awaitTermination()


