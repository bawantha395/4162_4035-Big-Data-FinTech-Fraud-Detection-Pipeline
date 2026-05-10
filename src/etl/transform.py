"""
ETL Step 2 – Transform
-----------------------
Cleans and aggregates the extracted rows.
Returns:
  - clean_rows  : list of dicts ready for loading into validated_ledger
  - summary     : dict with aggregated totals used for reconciliation
"""

import os, sys
from decimal import Decimal
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd


def transform(rows: list[dict]) -> tuple[list[dict], dict]:
    if not rows:
        print("[etl:transform] no data to transform")
        return [], {
            "total_clean_amount": 0.0,
            "tx_count": 0,
            "by_merchant": [],
            "by_country": [],
        }

    df = pd.DataFrame(rows)

    # Cast amount safely (may come back as Decimal from psycopg2)
    df["amount"] = df["amount"].apply(lambda x: float(x) if x else 0.0)

    # Aggregations
    by_merchant = (
        df.groupby("merchant_category", as_index=False)
        .agg(tx_count=("txn_ref", "count"), total_amount=("amount", "sum"))
        .sort_values("total_amount", ascending=False)
        .to_dict(orient="records")
    )

    by_country = (
        df.groupby("country", as_index=False)
        .agg(tx_count=("txn_ref", "count"), total_amount=("amount", "sum"))
        .sort_values("total_amount", ascending=False)
        .to_dict(orient="records")
    )

    summary = {
        "total_clean_amount": round(float(df["amount"].sum()), 2),
        "tx_count": len(df),
        "by_merchant": by_merchant,
        "by_country":  by_country,
    }

    print(
        f"[etl:transform] {summary['tx_count']} rows | "
        f"total ${summary['total_clean_amount']:,.2f}"
    )
    return df.to_dict(orient="records"), summary
