#!/bin/bash

# ========= Environment Variables =========
export KAFKA_BOOTSTRAP=localhost:9094
export INPUT_TOPIC=processed-log
export OUTPUT_TOPIC=anomalies
export CHECKPOINT=file:///tmp/checkpoints/logbert_v2
export SEQ_LEN=20
export THRESH=0.8
export LOGBERT_MODEL="/home/gowth/Documents/BDA/models/logbert.pt"  # adjust if needed

# Path to virtual environment activation
VENV_ACTIVATE="/home/gowth/Documents/BDA/python/venv/bin/activate.fish"

# ========= Launch scripts in parallel, each in a new Kitty window =========
kitty --detach fish -c "source $VENV_ACTIVATE; cd /home/gowth/Documents/BDA/python; python3 drain_3.py" &
kitty --detach fish -c "source $VENV_ACTIVATE; cd /home/gowth/Documents/BDA/python; python3 spark_logbert_streaming.py" &

wait  # optional: waits for both to finish
