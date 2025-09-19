# Big Data Log Anomaly Detection Pipeline

This project implements a pipeline where:
- Scala sends logs to Apache Kafka.
- Python uses Drain3 to extract log templates.
- Python runs LogBERT for anomaly detection.

## Setup Instructions

### 1. Start Kafka
```bash
cd kafka
docker-compose up -d
