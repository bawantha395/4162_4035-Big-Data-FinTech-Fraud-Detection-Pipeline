#!/usr/bin/env bash
# Spark Kafka Detector – Launched via spark-submit for proper dependency resolution

export SPARK_LOCAL_IP_ADDRESS=127.0.0.1
export SPARK_CHECKPOINT_DIR="${SPARK_CHECKPOINT_DIR:-/tmp/bs_checkpoints}"

# Get this script's directory  
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DETECTOR_PY="$SCRIPT_DIR/stream_detector_core.py"

# Run with spark-submit to let it handle package resolution
spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.sql.shuffle.partitions=4 \
  --conf "spark.sql.streaming.checkpointLocation=$SPARK_CHECKPOINT_DIR" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.1" \
  --repositories "https://repo1.maven.org/maven2/" \
  "$DETECTOR_PY"
