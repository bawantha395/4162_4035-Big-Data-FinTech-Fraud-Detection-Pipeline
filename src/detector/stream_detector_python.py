"""
BankSecure – Spark Structured Streaming Fraud Detector
=======================================================
Consumes the ``bank_transactions`` Kafka topic, applies two fraud rules,
and writes flagged events to PostgreSQL (``flagged_transactions`` table)
while all transactions land in ``all_transactions``.

Fraud rules
-----------
1. HIGH_VALUE        – amount > $5 000 in a single transaction
2. IMPOSSIBLE_TRAVEL – same account_id seen in 2 different countries
                       within a 10-minute tumbling window

Run
---
    python -m src.detector.stream_detector
"""

from __future__ import annotations

import os
import sys

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, BooleanType, StructType,
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_TXN,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_DRIVER_MEM,
    SPARK_EXECUTOR_MEM, SPARK_SHUFFLE_PARTS, SPARK_CHECKPOINT_DIR,
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD,
    HIGH_VALUE_THRESHOLD,
)

# ── Spark session ─────────────────────────────────────────────────────────────

_JARS_DIR = os.path.join(os.path.dirname(__file__), "../../jars")
_KAFKA_JAR    = os.path.join(_JARS_DIR, "spark-sql-kafka-0-10_2.13-4.1.1.jar")
_POSTGRES_JAR = os.path.join(_JARS_DIR, "postgresql-42.7.1.jar")


def _get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory",            SPARK_DRIVER_MEM)
        .config("spark.executor.memory",           SPARK_EXECUTOR_MEM)
        .config("spark.sql.shuffle.partitions",    str(SPARK_SHUFFLE_PARTS))
        .config("spark.sql.streaming.checkpointLocation", SPARK_CHECKPOINT_DIR)
    )

    packages = (
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
        "org.postgresql:postgresql:42.7.1"
    )
    builder = builder.config("spark.jars.packages", packages)
    builder = builder.config("spark.jars.repositories", "https://repo1.maven.org/maven2")

    if os.path.exists(_KAFKA_JAR) and os.path.exists(_POSTGRES_JAR):
        jars = f"{_KAFKA_JAR},{_POSTGRES_JAR}"
        builder = builder.config("spark.jars", jars)
        print("[detector] Using local jars with spark.jars.packages:", packages)
    else:
        print("[detector] Local jars missing; using spark.jars.packages:", packages)

    return builder.getOrCreate()


# ── Schema ────────────────────────────────────────────────────────────────────

TXN_SCHEMA = (
    StructType()
    .add("txn_ref",           StringType())
    .add("account_id",        StringType())
    .add("event_ts",          StringType())
    .add("merchant_category", StringType())
    .add("amount",            DoubleType())
    .add("country",           StringType())
    .add("channel",           StringType())
    .add("currency",          StringType())
    .add("flagged",           BooleanType())
    .add("flag_reason",       StringType())
)


# ── PostgreSQL writer ─────────────────────────────────────────────────────────

def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def _write_batch(df: DataFrame, table: str, on_conflict: str = "") -> None:
    if df.isEmpty():
        return
    rows = df.collect()
    cols = df.columns
    ph   = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    sql  = f"INSERT INTO {table} ({col_list}) VALUES ({ph}) {on_conflict}"

    conn   = _pg_conn()
    cursor = conn.cursor()
    for row in rows:
        try:
            cursor.execute(sql, tuple(row))
        except Exception as exc:
            print(f"[detector] db insert error ({table}): {exc}")
    conn.commit()
    cursor.close()
    conn.close()


def _sink_all_txn(df: DataFrame, epoch_id: int) -> None:
    _write_batch(
        df.select("txn_ref","account_id","event_ts","merchant_category",
                  "amount","country","channel","currency","proc_ts"),
        table="all_transactions",
        on_conflict="ON CONFLICT (txn_ref) DO NOTHING",
    )


def _sink_flagged(df: DataFrame, epoch_id: int) -> None:
    _write_batch(
        df.select("txn_ref","account_id","fraud_type","detection_ts","details"),
        table="flagged_transactions",
    )


# ── Streaming logic ───────────────────────────────────────────────────────────

def main() -> None:
    spark = _get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── ingest from Kafka ──────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_TXN)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", TXN_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn("proc_ts",    F.current_timestamp())
    )

    # ── Rule 1 : high-value ────────────────────────────────────────────────
    high_value_alerts = (
        parsed
        .filter(F.col("amount") > HIGH_VALUE_THRESHOLD)
        .withColumn("fraud_type",    F.lit("HIGH_VALUE"))
        .withColumn("detection_ts",  F.current_timestamp())
        .withColumn("details",
            F.to_json(F.struct(
                F.col("amount"),
                F.col("country"),
                F.col("merchant_category"),
            ))
        )
    )

    # ── Rule 2 : impossible travel (10-minute window, ≥ 2 countries) ──────
    windowed = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "10 minutes", "5 minutes"),
            F.col("account_id"),
        )
        .agg(
            F.collect_set("country").alias("countries"),
            F.collect_list("txn_ref").alias("txn_refs"),
            F.max("proc_ts").alias("last_seen"),
        )
        .filter(F.size("countries") > 1)
    )

    travel_alerts = (
        windowed
        .select(
            F.explode("txn_refs").alias("txn_ref"),
            F.col("account_id"),
            F.lit("IMPOSSIBLE_TRAVEL").alias("fraud_type"),
            F.current_timestamp().alias("detection_ts"),
            F.to_json(F.struct("window", "countries")).alias("details"),
        )
    )

    # ── combine alert streams ──────────────────────────────────────────────
    all_alerts = high_value_alerts.select(
        "txn_ref","account_id","fraud_type","detection_ts","details"
    ).union(travel_alerts)

    # ── write queries ──────────────────────────────────────────────────────
    q1 = (
        parsed
        .select("txn_ref","account_id","event_ts","merchant_category",
                "amount","country","channel","currency","proc_ts")
        .writeStream
        .foreachBatch(_sink_all_txn)
        .outputMode("append")
        .option("checkpointLocation", SPARK_CHECKPOINT_DIR + "/all_txn")
        .start()
    )

    q2 = (
        all_alerts
        .writeStream
        .foreachBatch(_sink_flagged)
        .outputMode("append")
        .option("checkpointLocation", SPARK_CHECKPOINT_DIR + "/flagged")
        .start()
    )

    print("[detector] streaming started – awaiting termination …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
