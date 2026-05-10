#!/usr/bin/env bash
# ============================================================
# BankSecure Pipeline – Full Run Script
# ============================================================
# Starts all services, runs producer + spark detector,
# then executes ETL and generates reports.
#
# Usage:
#   chmod +x run_pipeline.sh
#   ./run_pipeline.sh [producer_duration_seconds]
# ============================================================

set -euo pipefail

DURATION=${1:-120}   # default 120 s producer run
VENV=".venv"

echo ""
echo "========================================================"
echo "  BankSecure Pipeline – Starting up"
echo "  Producer duration: ${DURATION}s"
echo "========================================================"

# ── Docker-aware defaults ────────────────────────────────────────────────
if [ -z "${KAFKA_BOOTSTRAP:-}" ]; then
  if docker compose -f docker/docker-compose.yaml ps --services --filter "status=running" 2>/dev/null | grep -q "^kafka$"; then
    export KAFKA_BOOTSTRAP="localhost:9092"
    echo "[config] KAFKA_BOOTSTRAP not set; using ${KAFKA_BOOTSTRAP} for host access"
  fi
fi

# ── 1. Virtual environment ────────────────────────────────────────────────
if [ ! -d "$VENV" ]; then
  echo "[setup] Creating virtual environment …"
  python3 -m venv "$VENV"
fi
source "$VENV/bin/activate"
pip install -q -r requirements.txt

# ── 2. Docker services ────────────────────────────────────────────────────
echo "[docker] Starting Kafka + PostgreSQL …"
docker compose -f docker/docker-compose.yaml up -d
echo "[docker] Waiting 15 s for services to be ready …"
sleep 15

# ── 3. Kafka topics ───────────────────────────────────────────────────────
echo "[kafka] Creating topics …"
python -m src.producer.kafka_setup

# ── 4. Spark detector (background) ───────────────────────────────────────
echo "[spark] Starting fraud detector in background …"
python -m src.detector.stream_detector &
SPARK_PID=$!
echo "[spark] PID: $SPARK_PID"
sleep 8   # allow Spark to connect to Kafka

# ── 5. Producer ───────────────────────────────────────────────────────────
echo "[producer] Emitting transactions for ${DURATION}s …"
python -m src.producer.producer "$DURATION"

# ── 6. Allow Spark to flush remaining micro-batches ───────────────────────
echo "[spark] Flushing last micro-batches (10 s) …"
sleep 10
kill "$SPARK_PID" 2>/dev/null || true

# ── 7. ETL ────────────────────────────────────────────────────────────────
echo "[etl] Running ETL pipeline …"
python airflow/run_etl.py

# ── 8. Reports ────────────────────────────────────────────────────────────
echo "[reports] Generating merchant fraud report …"
python -m src.reports.merchant_report

echo "[reports] Generating full analytical report …"
python -m src.reports.full_report

echo ""
echo "========================================================"
echo "  ✓  Pipeline complete. Deliverables in ./deliverables/"
echo "========================================================"
