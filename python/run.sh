#!/bin/bash

# ========= Environment Variables =========
export KAFKA_BOOTSTRAP=localhost:9094
export INPUT_TOPIC=processed-log
export OUTPUT_TOPIC=anomalies
export CHECKPOINT=file:///tmp/checkpoints/logbert_v2 #checkpoint location
export SEQ_LEN=20
export THRESH=0.5
export LOGBERT_MODEL='path_to_logbert_model'  # adjust if needed

# Path to virtual environment activation
VENV_ACTIVATE="path_to_venv/"

# ========= Launch scripts in parallel, each in a new Kitty window =========
kitty --detach fish -c "source $VENV_ACTIVATE; cd path_to_the_drain3_code; python3 drain_3.py" &
kitty --detach fish -c "source $VENV_ACTIVATE; cd path_to_the_spark_code; python3 spark_logbert_streaming.py" &

wait  # optional: waits for both to finish


#Note: these script might change with respect to the terminal you use. I'm using kitty so I used the keyword kitty to detach new terminals.