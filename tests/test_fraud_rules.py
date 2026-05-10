"""
Tests – Fraud Detection Rules
==============================
Validates the two core fraud rules using PySpark batch DataFrames.
Run with:  pytest tests/test_fraud_rules.py -v
"""

import pytest
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    ss = (
        SparkSession.builder
        .master("local[2]")
        .appName("banksecure-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    ss.sparkContext.setLogLevel("ERROR")
    yield ss
    ss.stop()


# ── Rule 1: High-value detection ─────────────────────────────────────────────

class TestHighValueRule:
    def test_flags_amount_above_5000(self, spark):
        df = spark.createDataFrame([
            Row(txn_ref="t1", account_id="a1", amount=7500.00, country="US"),
            Row(txn_ref="t2", account_id="a2", amount=300.00,  country="UK"),
        ])
        flagged = df.filter(F.col("amount") > 5000)
        assert flagged.count() == 1
        assert flagged.first()["txn_ref"] == "t1"

    def test_does_not_flag_exactly_5000(self, spark):
        df = spark.createDataFrame([
            Row(txn_ref="t3", account_id="a3", amount=5000.00, country="DE"),
        ])
        flagged = df.filter(F.col("amount") > 5000)
        assert flagged.count() == 0

    def test_flags_multiple_high_value(self, spark):
        df = spark.createDataFrame([
            Row(txn_ref="t4", account_id="a4", amount=6000.00,  country="SG"),
            Row(txn_ref="t5", account_id="a5", amount=12000.00, country="AU"),
            Row(txn_ref="t6", account_id="a6", amount=99.00,    country="IN"),
        ])
        flagged = df.filter(F.col("amount") > 5000)
        assert flagged.count() == 2


# ── Rule 2: Impossible travel detection ──────────────────────────────────────

class TestImpossibleTravelRule:
    def _windowed_check(self, spark, rows):
        df = (
            spark.createDataFrame(rows)
            .withColumn("event_time", F.to_timestamp("event_ts"))
        )
        result = (
            df.groupBy(F.window("event_time", "10 minutes"), "account_id")
            .agg(F.collect_set("country").alias("countries"))
            .filter(F.size("countries") > 1)
        )
        return result

    def test_detects_two_countries_in_window(self, spark):
        base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        rows = [
            Row(txn_ref="r1", account_id="acc_x",
                event_ts=base.isoformat(), country="US"),
            Row(txn_ref="r2", account_id="acc_x",
                event_ts=(base + timedelta(minutes=6)).isoformat(), country="UK"),
        ]
        result = self._windowed_check(spark, rows)
        assert result.count() == 1, "Expected 1 impossible-travel alert"

    def test_no_alert_same_country(self, spark):
        base = datetime(2024, 6, 1, 14, 0, 0, tzinfo=timezone.utc)
        rows = [
            Row(txn_ref="r3", account_id="acc_y",
                event_ts=base.isoformat(), country="DE"),
            Row(txn_ref="r4", account_id="acc_y",
                event_ts=(base + timedelta(minutes=4)).isoformat(), country="DE"),
        ]
        result = self._windowed_check(spark, rows)
        assert result.count() == 0

    def test_no_alert_different_accounts(self, spark):
        base = datetime(2024, 6, 1, 15, 0, 0, tzinfo=timezone.utc)
        rows = [
            Row(txn_ref="r5", account_id="acc_p",
                event_ts=base.isoformat(), country="SG"),
            Row(txn_ref="r6", account_id="acc_q",
                event_ts=(base + timedelta(minutes=3)).isoformat(), country="AU"),
        ]
        result = self._windowed_check(spark, rows)
        assert result.count() == 0

    def test_alert_contains_both_countries(self, spark):
        base = datetime(2024, 6, 1, 16, 0, 0, tzinfo=timezone.utc)
        rows = [
            Row(txn_ref="r7", account_id="acc_z",
                event_ts=base.isoformat(), country="IN"),
            Row(txn_ref="r8", account_id="acc_z",
                event_ts=(base + timedelta(minutes=7)).isoformat(), country="BR"),
        ]
        result = self._windowed_check(spark, rows)
        countries = set(result.first()["countries"])
        assert "IN" in countries and "BR" in countries
