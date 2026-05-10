"""
BankTransaction – data model
-----------------------------
Plain Python dataclass representing one credit/debit card event
as it flows through the pipeline.
"""

from __future__ import annotations
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional


@dataclass
class BankTransaction:
    txn_ref:           str
    account_id:        str
    event_ts:          str          # ISO-8601 UTC
    merchant_category: str
    amount:            float
    country:           str
    channel:           str          # e.g. "ATM", "Online", "POS"
    currency:          str = "USD"
    flagged:           bool = False
    flag_reason:       Optional[str] = None

    # ── factories ─────────────────────────────────────────────────────────────

    @classmethod
    def new(
        cls,
        account_id: str,
        merchant_category: str,
        amount: float,
        country: str,
        channel: str,
        ts: Optional[datetime] = None,
    ) -> "BankTransaction":
        if ts is None:
            ts = datetime.now(timezone.utc)
        return cls(
            txn_ref=str(uuid.uuid4()),
            account_id=account_id,
            event_ts=ts.isoformat(),
            merchant_category=merchant_category,
            amount=round(amount, 2),
            country=country,
            channel=channel,
        )

    # ── serialisation ─────────────────────────────────────────────────────────

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), default=str).encode("utf-8")

    @classmethod
    def from_dict(cls, d: dict) -> "BankTransaction":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def __repr__(self) -> str:
        flag_str = f" ⚠ {self.flag_reason}" if self.flagged else ""
        return (
            f"[{self.event_ts[:19]}] {self.account_id[:8]} | "
            f"{self.merchant_category:<18} | {self.country} | "
            f"${self.amount:>9.2f}{flag_str}"
        )
