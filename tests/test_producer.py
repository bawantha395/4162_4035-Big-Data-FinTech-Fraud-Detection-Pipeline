"""
Tests – Transaction Producer
==============================
Run with:  pytest tests/test_producer.py -v
"""

import pytest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.producer.models import BankTransaction
from datetime import datetime, timezone, timedelta


class TestBankTransaction:
    def test_factory_creates_valid_txn(self):
        txn = BankTransaction.new(
            account_id="acct-001",
            merchant_category="ATM Withdrawal",
            amount=250.0,
            country="US",
            channel="ATM",
        )
        assert txn.txn_ref is not None
        assert txn.amount == 250.0
        assert txn.flagged is False
        assert txn.flag_reason is None

    def test_serialise_deserialise(self):
        txn = BankTransaction.new("acct-002", "Online Payment", 99.99, "UK", "Online")
        data = txn.to_bytes()
        import json
        d = json.loads(data)
        assert d["account_id"] == "acct-002"
        assert d["amount"] == 99.99

    def test_amount_is_rounded(self):
        txn = BankTransaction.new("acct-003", "POS Terminal", 123.456789, "DE", "POS")
        assert txn.amount == 123.46

    def test_fraud_flag_can_be_set(self):
        txn = BankTransaction.new("acct-004", "Wire Transfer", 9999.0, "NG", "Branch")
        txn.flagged = True
        txn.flag_reason = "HIGH_VALUE"
        assert txn.flagged is True
        assert txn.flag_reason == "HIGH_VALUE"

    def test_repr_contains_amount(self):
        txn = BankTransaction.new("acct-005", "Forex Exchange", 500.0, "SG", "Mobile")
        assert "500.00" in repr(txn)


class TestImpossibleTravelLogic:
    """Validate the travel-fraud generation logic without Kafka."""

    def test_new_country_differs_from_previous(self):
        from src.producer.producer import _impossible_travel_pair, ACCOUNTS
        prev = BankTransaction.new(ACCOUNTS[0], "ATM Withdrawal", 100.0, "US", "ATM")
        follow = _impossible_travel_pair(ACCOUNTS[0], prev)
        assert follow.country != "US"

    def test_follow_ts_within_window(self):
        from src.producer.producer import _impossible_travel_pair, ACCOUNTS
        from config.settings import TRAVEL_WINDOW_MIN
        prev = BankTransaction.new(ACCOUNTS[1], "Online Payment", 200.0, "UK", "Online")
        follow = _impossible_travel_pair(ACCOUNTS[1], prev)
        prev_ts   = datetime.fromisoformat(prev.event_ts)
        follow_ts = datetime.fromisoformat(follow.event_ts)
        diff_min  = (follow_ts - prev_ts).total_seconds() / 60
        assert 0 < diff_min < TRAVEL_WINDOW_MIN
