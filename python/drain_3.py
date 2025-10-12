# drain3.py
# pip install kafka-python drain3

from kafka import KafkaConsumer, KafkaProducer
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import MaskingInstruction
from drain3.file_persistence import FilePersistence
from datetime import datetime, timezone
import json
import sys
import signal
import re


# --- Config ---
BOOTSTRAP     = "localhost:9094"   # Kafka broker (host listener)
INPUT_TOPIC   = "logs"
OUTPUT_TOPIC  = "processed-log"    # processed logs for LogBERT
GROUP_ID      = "drain3-bridge"
STATE_PATH    = "./drain3_state.bin"  # persists clusters across restarts
FLUSH_EVERY_N = 1  # set to >1 if you want to batch-produce (e.g., 20)

# ---- Drain3 config with useful masking ----
cfg = TemplateMinerConfig()  # loads defaults
cfg.masking = [
    MaskingInstruction(r"\b\d+\b", "<NUM>"),                         # numbers
    MaskingInstruction(r"\bE\d+\b", "E<NUM>"),                       # E-codes like E91
    MaskingInstruction(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "<IP>"),      # IPv4
    MaskingInstruction(r"\b0x[0-9a-fA-F]+\b", "<HEX>"),              # hex with 0x
    MaskingInstruction(r"\b[0-9a-fA-F]{8,}\b", "<HEX>"),             # long hex blobs
]
persistence = FilePersistence(STATE_PATH)
template_miner = TemplateMiner(persistence, cfg)

# ---- Kafka consumer/producer ----
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda m: m.decode("utf-8"),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k if isinstance(k, (bytes, bytearray)) else str(k).encode("utf-8"),
)

print(f"Starting Drain3 bridge: {INPUT_TOPIC} -> {OUTPUT_TOPIC}", file=sys.stderr)

_running = True
def _graceful_shutdown(signum, frame):
    global _running
    _running = False
signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)

# --- Helpers ---
SPLIT_RE = re.compile(r"[,\s;]+")

def split_events(s: str):
    """
    Split multi-event strings on comma/semicolon/whitespace, trim, drop empties, preserve order, de-dup.
    Example: 'E91,E92  E93;E91' -> ['E91', 'E92', 'E93']
    """
    if not s:
        return []
    parts = [p for p in SPLIT_RE.split(s) if p]
    seen, out = set(), []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

def extract_msg_text(raw_json: str) -> str:
    """Prefer raw_log.events[0]; fall back to known fields; last resort: whole payload."""
    try:
        payload = json.loads(raw_json)
    except Exception:
        return (raw_json or "").strip()

    if isinstance(payload, dict):
        rl = payload.get("raw_log", {})
        if isinstance(rl, dict):
            ev = rl.get("events", [])
            if isinstance(ev, list) and ev:
                t = (ev[0] or "").strip()
                if t:
                    return t
        for key in ("text", "message"):
            t = (payload.get(key, "") or "").strip()
            if t:
                return t
        return (raw_json or "").strip()
    return (raw_json or "").strip()

def mine_template(msg_text: str):
    """Run Drain3 and normalize the result keys."""
    try:
        res = template_miner.add_log_message(msg_text) or {}
    except Exception as e:
        print(f"[Drain3] ERROR: {e}", file=sys.stderr)
        res = {}

    # Newer Drain3 uses 'template_mined'; older examples used 'template'
    template = (res.get("template_mined") or res.get("template") or "") or ""
    cluster_raw = res.get("cluster_id", -1)
    try:
        template_id = int(cluster_raw)
    except Exception:
        template_id = -1
    return template, template_id

# ---- Main loop ----
to_flush = 0
while _running:
    # Iterate message-by-message so we can stop promptly
    for message in consumer:
        if not _running:
            break

        raw_json = message.value
        src_key_str = message.key or "web"

        # Spark-friendly timestamp (no offset; "yyyy-MM-dd HH:mm:ss")
        ts_iso = datetime.fromtimestamp(message.timestamp / 1000, tz=timezone.utc)\
                         .strftime("%Y-%m-%d %H:%M:%S")

        # Extract and split events (e.g., "E91,E92" -> ["E91","E92"])
        msg_text = extract_msg_text(raw_json)
        if not msg_text:
            continue

        events = split_events(msg_text) or [msg_text]

        for ev_text in events:
            template, template_id = mine_template(ev_text)

            feature_data = {
                "raw_log": ev_text,
                "original_json": raw_json,
                "template": template,
                "template_id": template_id,
                "source_key": src_key_str,
                "ts": ts_iso,
                # NEW: pass-through label from the original nested JSON
                "label": (json.loads(raw_json).get("raw_log", {}) if raw_json else {}).get("label")
            }

            print("Drain3 →", feature_data, file=sys.stderr)

            # Produce to processed-log
            try:
                producer.send(OUTPUT_TOPIC, key=src_key_str, value=feature_data)
                to_flush += 1
                if to_flush >= FLUSH_EVERY_N:
                    producer.flush()
                    to_flush = 0
            except Exception as e:
                print(f"[Kafka] produce error: {e}", file=sys.stderr)

    # If the inner iterator exhausted (rebalance/EOF), loop continues unless stopped

# Shutdown
try:
    if to_flush:
        producer.flush()
    producer.close()
except Exception:
    pass
try:
    consumer.close()
except Exception:
    pass
print("Drain3 bridge stopped.", file=sys.stderr)

