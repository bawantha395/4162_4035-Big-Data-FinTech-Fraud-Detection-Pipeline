"""
Merchant Fraud Report
----------------------
Analytic report: "Fraud Attempts by Merchant Category"

Queries flagged_transactions joined with all_transactions,
groups by merchant_category, and writes:
  - Console table
  - CSV  → deliverables/fraud_by_merchant_<ts>.csv
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from database.db import fetch_all

_SQL = """
    SELECT
        a.merchant_category,
        f.fraud_type,
        COUNT(*)                    AS fraud_count,
        COALESCE(SUM(a.amount), 0)  AS total_fraud_amount
    FROM   flagged_transactions f
    JOIN   all_transactions a ON f.txn_ref = a.txn_ref
    GROUP  BY a.merchant_category, f.fraud_type
    ORDER  BY fraud_count DESC
"""

_OUT_DIR = os.path.join(os.path.dirname(__file__), "../../deliverables")


def generate_merchant_fraud_report() -> str:
    rows = fetch_all(_SQL)

    if not rows:
        print("[report] no fraud data available yet")
        return ""

    # ── console ────────────────────────────────────────────────────────────
    header = f"{'Merchant Category':<22} {'Fraud Type':<22} {'Count':>6}  {'Total ($)':>14}"
    sep    = "-" * len(header)
    print(f"\n{'='*60}")
    print("  FRAUD ATTEMPTS BY MERCHANT CATEGORY")
    print(f"{'='*60}")
    print(header)
    print(sep)
    for r in rows:
        print(
            f"{r['merchant_category']:<22} {r['fraud_type']:<22} "
            f"{r['fraud_count']:>6}  ${r['total_fraud_amount']:>13,.2f}"
        )
    print(f"{'='*60}\n")

    # ── CSV ────────────────────────────────────────────────────────────────
    os.makedirs(_OUT_DIR, exist_ok=True)
    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(_OUT_DIR, f"fraud_by_merchant_{ts}.csv")

    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["merchant_category", "fraud_type",
                        "fraud_count", "total_fraud_amount"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"[report] CSV saved → {csv_path}")
    return csv_path


if __name__ == "__main__":
    generate_merchant_fraud_report()
