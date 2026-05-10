"""
BankSecure Pipeline – Central Configuration
============================================
All environment-driven settings live here. Import this module
wherever connection details or tuning knobs are needed.
"""

import os

# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_TXN    = os.getenv("KAFKA_TOPIC_TXN",   "bank_transactions")
KAFKA_TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "security_alerts")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID",     "banksecure-consumer")
KAFKA_PARTITIONS   = int(os.getenv("KAFKA_PARTITIONS",   "3"))
KAFKA_REPLICATION  = int(os.getenv("KAFKA_REPLICATION",  "1"))

# ── PostgreSQL ────────────────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB",       "banksecure_db")
PG_USER     = os.getenv("PG_USER",     "bs_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "bs_pass")
PG_DSN      = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME       = "BankSecure-FraudDetector"
SPARK_MASTER         = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEM     = os.getenv("SPARK_DRIVER_MEM",   "2g")
SPARK_EXECUTOR_MEM   = os.getenv("SPARK_EXECUTOR_MEM", "2g")
SPARK_SHUFFLE_PARTS  = int(os.getenv("SPARK_SHUFFLE_PARTS", "4"))
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/bs_checkpoints")

# ── Warehouse (Parquet) ───────────────────────────────────────────────────────
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "./warehouse/validated")

# ── Producer tuning ───────────────────────────────────────────────────────────
TX_PER_SECOND       = float(os.getenv("TX_PER_SECOND", "8"))
HIGH_VALUE_THRESHOLD = float(os.getenv("HIGH_VALUE_THRESHOLD", "5000.0"))
FRAUD_INJECT_RATE   = float(os.getenv("FRAUD_INJECT_RATE", "0.12"))   # ~12 %
TRAVEL_FRAUD_SHARE  = float(os.getenv("TRAVEL_FRAUD_SHARE",  "0.65")) # of fraud
HIGH_VALUE_SHARE    = float(os.getenv("HIGH_VALUE_SHARE",    "0.35")) # of fraud
TRAVEL_WINDOW_MIN   = int(os.getenv("TRAVEL_WINDOW_MIN", "10"))

# ── Simulation data ───────────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "Retail Banking", "Wire Transfer", "ATM Withdrawal",
    "Online Payment", "POS Terminal", "Mobile Wallet",
    "Forex Exchange", "Loan Repayment",
]
LOCATIONS = ["US", "UK", "DE", "SG", "AU", "IN", "NG", "BR"]
NUM_SIMULATED_USERS = 60
