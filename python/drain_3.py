from kafka import KafkaConsumer, KafkaProducer
from drain3 import TemplateMiner
from datetime import datetime
import json

# --- Config (adjust if needed) ---
BOOTSTRAP = "localhost:9094"       # host -> EXTERNAL listener from your docker-compose
INPUT_TOPIC = "logs"
OUTPUT_TOPIC = "processed-log"     # <-- underscore, as requested
GROUP_ID = "drain3-bridge"

template_miner = TemplateMiner()

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",   # helpful while testing
    enable_auto_commit=True,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda m: m.decode("utf-8"),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    # We'll pass a dict as value; this serializer emits JSON bytes.
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # No key_serializer: we will forward bytes (or provide bytes explicitly)
)

print(f"Starting Drain3 bridge: {INPUT_TOPIC} -> {OUTPUT_TOPIC}")

for message in consumer:
    raw_log = message.value
    src_key_str = message.key or "web"  # decoded string (see key_deserializer above)
    src_key_bytes = (message.key.encode("utf-8") if isinstance(message.key, str)
                     else (message.key or b"web"))  # bytes to forward as Kafka key

    # Kafka record timestamp -> ISO string (UTC)
    ts_iso = datetime.utcfromtimestamp(message.timestamp / 1000)\
                     .isoformat(sep=" ", timespec="seconds")

    # Drain3 template mining
    result = template_miner.add_log_message(raw_log) or {}
    template = result.get("template", "")
    template_id = int(result.get("cluster_id", -1))

    feature_data = {
        "raw_log": raw_log,
        "template": template,
        "template_id": template_id,
        "source_key": src_key_str,   # for per-source sequences in Spark/LogBERT
        "ts": ts_iso
    }

    print("Drain3 →", feature_data)

    # Send to processed_log with the same Kafka key (bytes)
    producer.send(OUTPUT_TOPIC, key=src_key_bytes, value=feature_data)
    producer.flush()

