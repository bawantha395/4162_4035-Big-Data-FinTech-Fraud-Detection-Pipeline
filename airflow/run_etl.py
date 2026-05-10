"""
Standalone ETL Runner
---------------------
Executes the full ETL pipeline without Airflow.
Useful for testing and for generating deliverables.

Usage:
    python run_etl.py
"""

import os, sys, json
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.etl.extract    import extract_clean_transactions
from src.etl.transform  import transform
from src.etl.load       import load
from src.etl.reconcile  import generate as reconcile

SEP = "=" * 60


def run() -> int:
    print(f"\n{SEP}")
    print(f"  BankSecure ETL – {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")
    print(SEP)

    try:
        rows              = extract_clean_transactions()
        clean_rows, summ  = transform(rows)
        load_result       = load(clean_rows)
        recon             = reconcile(summ)

        print(f"\n{SEP}")
        print("  ✓ ETL completed successfully")
        print(f"  Parquet → {load_result.get('parquet_path','n/a')}")
        print(f"  Recon status: {recon['status']}")
        print(SEP + "\n")
        return 0

    except Exception as exc:
        import traceback
        print(f"\n[ETL] FAILED: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(run())
