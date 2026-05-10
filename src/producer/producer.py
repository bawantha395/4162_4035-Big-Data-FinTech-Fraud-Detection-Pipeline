"""
BankSecure Transaction Producer
================================
Simulates a real-time digital-wallet transaction feed and publishes
each event to the Kafka topic ``bank_transactions``.

Fraud injection strategy
------------------------
* ~65 % of fraud budget  → impossible-travel  (same account, 2 different
                            countries within TRAVEL_WINDOW_MIN minutes)
* ~35 % of fraud budget  → high-value         (single txn amount > $5 000)
* Remaining              → normal transactions

Usage
-----
    python -m src.producer.producer          # run for 1 hour
    python -m src.producer.producer 120      # run for 120 seconds
"""

from __future__ import annotations

import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_TXN,
    TX_PER_SECOND, FRAUD_INJECT_RATE,
    TRAVEL_FRAUD_SHARE, HIGH_VALUE_SHARE,
    HIGH_VALUE_THRESHOLD, TRAVEL_WINDOW_MIN,
    MERCHANT_CATEGORIES, LOCATIONS, NUM_SIMULATED_USERS,
)
from src.producer.kafka_setup import ensure_topics
from src.producer.models import BankTransaction

CHANNELS = ["ATM", "Online", "POS", "Mobile", "Branch"]


# ── account pool ──────────────────────────────────────────────────────────────

ACCOUNTS: list[str] = [str(uuid.uuid4()) for _ in range(NUM_SIMULATED_USERS)]


# ── transaction factories ─────────────────────────────────────────────────────

def _random_normal(account_id: str, ts: Optional[datetime] = None) -> BankTransaction:
    """Generate a legitimate low-risk transaction ($10 – $999)."""
    return BankTransaction.new(
        account_id=account_id,
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        amount=round(random.uniform(10.0, 999.0), 2),
        country=random.choice(LOCATIONS),
        channel=random.choice(CHANNELS),
        ts=ts,
    )


def _high_value_fraud(account_id: str) -> BankTransaction:
    """Generate a single high-value suspicious transaction (> $5 000)."""
    txn = BankTransaction.new(
        account_id=account_id,
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        amount=round(random.uniform(HIGH_VALUE_THRESHOLD + 0.01, 18_000.0), 2),
        country=random.choice(LOCATIONS),
        channel=random.choice(CHANNELS),
    )
    txn.flagged = True
    txn.flag_reason = "HIGH_VALUE"
    return txn


def _impossible_travel_pair(
    account_id: str, previous: BankTransaction
) -> BankTransaction:
    """
    Generate a follow-up transaction in a *different* country within
    TRAVEL_WINDOW_MIN minutes – physically impossible to travel that fast.
    """
    prev_country = previous.country
    new_country = random.choice([c for c in LOCATIONS if c != prev_country])
    prev_ts = datetime.fromisoformat(previous.event_ts)
    offset  = timedelta(minutes=random.uniform(2.0, TRAVEL_WINDOW_MIN - 0.5))
    new_ts  = prev_ts + offset

    txn = BankTransaction.new(
        account_id=account_id,
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        amount=round(random.uniform(50.0, 4_000.0), 2),
        country=new_country,
        channel=random.choice(CHANNELS),
        ts=new_ts,
    )
    txn.flagged = True
    txn.flag_reason = "IMPOSSIBLE_TRAVEL"
    return txn


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER if (KAFKA_BROKER := KAFKA_BOOTSTRAP) else KAFKA_BOOTSTRAP,
        linger_ms=10,
        acks="all",
        retries=3,
        request_timeout_ms=20_000,
    )


def _send(producer: KafkaProducer, txn: BankTransaction) -> bool:
    try:
        producer.send(KAFKA_TOPIC_TXN, value=txn.to_bytes())
        _log(txn)
        return True
    except KafkaError as exc:
        print(f"[producer] send error: {exc}", file=sys.stderr)
        return False


def _log(txn: BankTransaction) -> None:
    prefix = "[FRAUD]" if txn.flagged else "[  OK ]"
    reason = f" ({txn.flag_reason})" if txn.flagged else ""
    print(f"{prefix} {txn}{reason}")


# ── main loop ─────────────────────────────────────────────────────────────────

def run(duration_s: int = 3_600) -> dict:
    """
    Emit transactions for *duration_s* seconds, then return summary stats.
    """
    ensure_topics()
    producer = _build_producer()

    # last known txn per account (for impossible-travel generation)
    history: Dict[str, BankTransaction] = {}

    stats = {"total": 0, "normal": 0, "high_value": 0, "impossible_travel": 0}
    interval = 1.0 / TX_PER_SECOND
    deadline = time.monotonic() + duration_s

    try:
        while time.monotonic() < deadline:
            account_id = random.choice(ACCOUNTS)
            roll = random.random()

            if roll < FRAUD_INJECT_RATE:
                # --- fraud branch ---
                fraud_roll = random.random()
                if fraud_roll < TRAVEL_FRAUD_SHARE and account_id in history:
                    txn = _impossible_travel_pair(account_id, history[account_id])
                    key = "impossible_travel"
                else:
                    txn = _high_value_fraud(account_id)
                    key = "high_value"
            else:
                # --- normal branch ---
                txn = _random_normal(account_id)
                key = "normal"

            if _send(producer, txn):
                history[account_id] = txn
                stats["total"] += 1
                stats[key] += 1

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[producer] interrupted by user")
    finally:
        producer.flush()
        producer.close()

    _print_summary(stats, duration_s)
    return stats


def _print_summary(stats: dict, duration_s: int) -> None:
    total = max(stats["total"], 1)
    sep = "=" * 55
    print(f"\n{sep}")
    print(f"  PRODUCER SUMMARY  ({duration_s}s run)")
    print(sep)
    print(f"  Total emitted       : {stats['total']}")
    print(f"  Normal              : {stats['normal']}  ({stats['normal']/total*100:.1f}%)")
    print(f"  High-value fraud    : {stats['high_value']}  ({stats['high_value']/total*100:.1f}%)")
    print(f"  Impossible-travel   : {stats['impossible_travel']}  ({stats['impossible_travel']/total*100:.1f}%)")
    print(sep + "\n")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 3_600
    run(secs)
