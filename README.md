# BankSecure Fraud Detection Pipeline

**Domain:** Banking & Security  
**Scenario:** Real-time credit/debit card fraud detection with batch reconciliation

---

## Overview

A digital wallet provider streams card transactions through Kafka. A PySpark
Structured Streaming job applies two fraud rules in real-time and pushes flagged
events to PostgreSQL. Every six hours Airflow runs an ETL job that moves clean
transactions into a Parquet data warehouse, reconciles ingress vs validated totals,
and produces a "Fraud by Merchant Category" analytics report.

---

## Fraud Rules

| Rule | Condition | Action |
|---|---|---|
| **HIGH_VALUE** | Single transaction `amount > $5,000` | Flag immediately |
| **IMPOSSIBLE_TRAVEL** | Same `account_id` in ≥ 2 different countries within 10 minutes | Flag immediately |

Fraud injection rate: ~12 % (65 % travel fraud, 35 % high-value).

---

## Project Layout

```
BankSecure_Pipeline/
├── config/
│   └── settings.py          # All env-driven config
├── src/
│   ├── producer/
│   │   ├── kafka_setup.py   # Topic creation
│   │   ├── models.py        # BankTransaction dataclass
│   │   └── producer.py      # Kafka producer / simulator
│   ├── detector/
│   │   └── stream_detector.py  # Spark Structured Streaming
│   ├── etl/
│   │   ├── extract.py       # Pull clean txns from Postgres
│   │   ├── transform.py     # Pandas aggregations
│   │   ├── load.py          # Write to Postgres + Parquet
│   │   └── reconcile.py     # Reconciliation report
│   └── reports/
│       ├── merchant_report.py  # Fraud by merchant CSV
│       └── full_report.py      # Full analytical report (txt)
├── airflow/
│   ├── dags/etl_dag.py      # Airflow 6-hourly DAG
│   └── run_etl.py           # Standalone ETL runner
├── database/
│   ├── schema.sql           # DB initialisation
│   └── db.py                # psycopg2 helpers
├── tests/
│   ├── test_fraud_rules.py  # Spark unit tests
│   ├── test_producer.py     # Producer unit tests
│   └── test_etl.py          # ETL transform tests
├── docker/
│   └── docker-compose.yml   # Kafka + Zookeeper + PostgreSQL
├── warehouse/               # Parquet files land here
├── deliverables/            # Reports and CSVs land here
├── requirements.txt
└── run_pipeline.sh          # One-shot full run
```

---

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
sleep 15   # wait for Kafka + Postgres to be ready
```

### 3. Create Kafka topics

```bash
python -m src.producer.kafka_setup
```

### 4. Start Spark fraud detector

```bash
python -m src.detector.stream_detector &
```

### 5. Run producer (120 seconds)

```bash
python -m src.producer.producer 120
```

### 6. Run ETL & reports

```bash
python airflow/run_etl.py
python -m src.reports.merchant_report
python -m src.reports.full_report
```

**Or run everything at once:**

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh 120
```

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker |
| `KAFKA_TOPIC_TXN` | `bank_transactions` | Transaction topic |
| `KAFKA_TOPIC_ALERTS` | `security_alerts` | Alert topic |
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5433` | PostgreSQL host port |
| `PG_DB` | `banksecure_db` | Database name |
| `PG_USER` | `bs_user` | DB username |
| `PG_PASSWORD` | `bs_pass` | DB password |
| `TX_PER_SECOND` | `8` | Producer emit rate |
| `HIGH_VALUE_THRESHOLD` | `5000.0` | Fraud threshold ($) |
| `FRAUD_INJECT_RATE` | `0.12` | Fraction of fraudulent txns |
| `WAREHOUSE_PATH` | `./warehouse/validated` | Parquet output path |

---

## Database Tables

| Table | Purpose |
|---|---|
| `all_transactions` | Every event ingested from Kafka |
| `flagged_transactions` | Fraud alerts with type and details |
| `validated_ledger` | Clean transactions after ETL |
| `reconciliation_log` | Per-run reconciliation results |
