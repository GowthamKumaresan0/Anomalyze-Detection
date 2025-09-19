# save as read_anomalies.py and run: python read_anomalies.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "anomalies",
    bootstrap_servers="localhost:9094",   # host port you mapped from Docker
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Consuming from 'anomalies'…")
for msg in consumer:
    print(msg.value)  # dict: {"timestamp": "...", "raw_log": "...", "template": "...", "score": 0.xx, "is_anomaly": true, "seq_len": 20}

