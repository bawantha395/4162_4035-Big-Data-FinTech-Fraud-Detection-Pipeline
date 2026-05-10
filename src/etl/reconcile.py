"""
ETL Step 4 – Reconciliation Report
------------------------------------
Compares Total Ingress Amount vs Validated Amount and logs the result.
Prints a formatted summary and persists one row to reconciliation_log.
"""

import os, sys
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from database.db import fetch_one, execute

_SEP = "=" * 60


def generate(summary: dict) -> dict:
    # ── totals from DB ────────────────────────────────────────────────────
    total_row  = fetch_one("SELECT COALESCE(SUM(amount),0) AS t FROM all_transactions")
    fraud_row  = fetch_one("""
        SELECT COALESCE(SUM(a.amount),0) AS t
        FROM   all_transactions a
        JOIN   flagged_transactions f ON a.txn_ref = f.txn_ref
    """)
    valid_row  = fetch_one("SELECT COALESCE(SUM(amount),0) AS t FROM validated_ledger")

    total_ingress = float(total_row["t"])  if total_row  else 0.0
    fraud_amount  = float(fraud_row["t"])  if fraud_row  else 0.0
    valid_amount  = float(valid_row["t"])  if valid_row  else 0.0

    expected_valid = round(total_ingress - fraud_amount, 2)
    diff           = round(abs(expected_valid - valid_amount), 2)
    status         = "PASS" if diff < 0.02 else "FAIL"

    # ── persist ───────────────────────────────────────────────────────────
    execute(
        """
        INSERT INTO reconciliation_log
            (total_ingested, fraud_amount, validated_amt, status)
        VALUES (%s, %s, %s, %s)
        """,
        (total_ingress, fraud_amount, valid_amount, status),
    )

    # ── print ─────────────────────────────────────────────────────────────
    print(f"\n{_SEP}")
    print(f"  RECONCILIATION REPORT  –  {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")
    print(_SEP)
    print(f"  Total ingested   : ${total_ingress:>15,.2f}")
    print(f"  Fraud detected   : ${fraud_amount:>15,.2f}")
    print(f"  Validated amount : ${valid_amount:>15,.2f}")
    print(f"  Expected valid   : ${expected_valid:>15,.2f}")
    print(f"  Difference       : ${diff:>15,.2f}")
    print(f"  Status           : {status}")
    print(_SEP + "\n")

    return {
        "total_ingress": total_ingress,
        "fraud_amount":  fraud_amount,
        "valid_amount":  valid_amount,
        "status":        status,
    }
