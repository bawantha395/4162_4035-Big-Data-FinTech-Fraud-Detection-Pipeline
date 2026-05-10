"""
Tests – ETL Transform & Reconciliation Logic
=============================================
Run with:  pytest tests/test_etl.py -v
"""

import pytest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.etl.transform import transform


class TestTransform:
    def _make_rows(self, amounts, categories=None):
        cats = categories or ["ATM Withdrawal"] * len(amounts)
        return [
            {
                "txn_ref": f"t{i}",
                "account_id": f"a{i}",
                "event_ts": "2024-01-01T10:00:00",
                "merchant_category": cats[i],
                "amount": amt,
                "country": "US",
                "channel": "ATM",
                "currency": "USD",
                "proc_ts": None,
            }
            for i, amt in enumerate(amounts)
        ]

    def test_empty_input_returns_empty(self):
        rows, summary = transform([])
        assert rows == []
        assert summary["tx_count"] == 0
        assert summary["total_clean_amount"] == 0.0

    def test_correct_total(self):
        rows, summary = transform(self._make_rows([100.0, 200.0, 300.0]))
        assert summary["total_clean_amount"] == 600.0
        assert summary["tx_count"] == 3

    def test_aggregation_by_merchant(self):
        rows, summary = transform(
            self._make_rows(
                [500.0, 300.0, 200.0],
                ["Online Payment", "ATM Withdrawal", "Online Payment"],
            )
        )
        by_m = {r["merchant_category"]: r["total_amount"]
                for r in summary["by_merchant"]}
        assert by_m["Online Payment"] == pytest.approx(700.0)
        assert by_m["ATM Withdrawal"] == pytest.approx(300.0)

    def test_row_count_matches_input(self):
        input_rows = self._make_rows([10.0, 20.0, 30.0, 40.0])
        clean_rows, _ = transform(input_rows)
        assert len(clean_rows) == 4

    def test_amounts_cast_to_float(self):
        from decimal import Decimal
        rows = self._make_rows([Decimal("123.45")])
        clean_rows, summary = transform(rows)
        assert isinstance(summary["total_clean_amount"], float)
