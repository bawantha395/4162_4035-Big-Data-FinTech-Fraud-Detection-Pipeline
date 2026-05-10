"""
ETL Step 1 – Extract
---------------------
Pull all transactions that have NOT been flagged as fraudulent.
Returns a list of row dicts ready for transform.
"""

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from database.db import fetch_all


_SQL = """
    SELECT a.*
    FROM   all_transactions a
    LEFT JOIN flagged_transactions f ON a.txn_ref = f.txn_ref
    WHERE  f.txn_ref IS NULL
"""


def extract_clean_transactions() -> list[dict]:
    rows = fetch_all(_SQL)
    print(f"[etl:extract] {len(rows)} clean (non-fraud) transactions fetched")
    return rows
