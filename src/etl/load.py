"""
ETL Step 3 – Load
------------------
Writes validated (clean) transactions to:
  1. PostgreSQL  → validated_ledger table
  2. Parquet     → ./warehouse/validated/  (partitioned by country)
"""

import os, sys
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from database.db import execute_many, get_conn
from config.settings import WAREHOUSE_PATH

_INSERT_SQL = """
    INSERT INTO validated_ledger
        (txn_ref, account_id, event_ts, merchant_category,
         amount, country, channel, currency, proc_ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (txn_ref) DO NOTHING
"""


def load_to_postgres(rows: list[dict]) -> int:
    if not rows:
        print("[etl:load] nothing to insert into validated_ledger")
        return 0

    params = [
        (
            r["txn_ref"], r["account_id"], r["event_ts"],
            r["merchant_category"], r["amount"], r["country"],
            r["channel"], r.get("currency", "USD"), r.get("proc_ts"),
        )
        for r in rows
    ]
    n = execute_many(_INSERT_SQL, params)
    print(f"[etl:load] {n} rows inserted into validated_ledger")
    return n


def load_to_parquet(rows: list[dict]) -> str:
    if not rows:
        return ""

    df = pd.DataFrame(rows)
    df["amount"] = df["amount"].apply(lambda x: float(x) if x else 0.0)

    run_date = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path  = os.path.join(WAREHOUSE_PATH, f"batch_{run_date}.parquet")
    os.makedirs(WAREHOUSE_PATH, exist_ok=True)

    df.to_parquet(out_path, index=False, engine="pyarrow")
    print(f"[etl:load] Parquet written → {out_path}  ({len(df)} rows)")
    return out_path


def load(rows: list[dict]) -> dict:
    pg_count   = load_to_postgres(rows)
    parquet_path = load_to_parquet(rows)
    return {"pg_inserted": pg_count, "parquet_path": parquet_path}
