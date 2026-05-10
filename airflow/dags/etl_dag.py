"""
BankSecure – Airflow ETL DAG
=============================
Runs every 6 hours.  Task order:
  extract  →  transform  →  load  →  reconcile  →  merchant_report

Schedule: 0 */6 * * *
"""

from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner":            "banksecure",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="banksecure_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="BankSecure 6-hourly ETL: extract → transform → load → reconcile",
    schedule_interval="0 */6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["banksecure", "etl", "fraud"],
) as dag:

    @task()
    def t_extract() -> list[dict]:
        from src.etl.extract import extract_clean_transactions
        return extract_clean_transactions()

    @task()
    def t_transform(rows: list[dict]) -> dict:
        from src.etl.transform import transform
        clean_rows, summary = transform(rows)
        return {"clean_rows": clean_rows, "summary": summary}

    @task()
    def t_load(payload: dict) -> dict:
        from src.etl.load import load
        return load(payload["clean_rows"])

    @task()
    def t_reconcile(payload: dict) -> dict:
        from src.etl.reconcile import generate
        return generate(payload["summary"])

    @task()
    def t_merchant_report(_: dict) -> None:
        from src.reports.merchant_report import generate_merchant_fraud_report
        generate_merchant_fraud_report()

    rows    = t_extract()
    payload = t_transform(rows)
    loaded  = t_load(payload)
    recon   = t_reconcile(payload)
    t_merchant_report(recon)
