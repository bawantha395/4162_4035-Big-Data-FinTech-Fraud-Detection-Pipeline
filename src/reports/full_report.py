"""
Analytical Report Generator
-----------------------------
Generates a comprehensive PDF/text report covering:
  1. Pipeline overview
  2. Transaction volume summary
  3. Fraud breakdown (type × merchant × country)
  4. Reconciliation result
  5. Top flagged accounts
"""

from __future__ import annotations

import os, sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from database.db import fetch_all, fetch_one

_OUT_DIR = os.path.join(os.path.dirname(__file__), "../../deliverables")


def _q(sql):
    return fetch_all(sql) or []


def generate_full_report() -> str:
    os.makedirs(_OUT_DIR, exist_ok=True)
    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(_OUT_DIR, f"full_report_{ts}.txt")

    lines: list[str] = []

    def h1(t):  lines.append("\n" + "=" * 65 + f"\n  {t}\n" + "=" * 65)
    def h2(t):  lines.append("\n" + "-" * 55 + f"\n  {t}\n" + "-" * 55)
    def row(*cols): lines.append("  " + "  ".join(str(c) for c in cols))

    h1("BANKSECURE PIPELINE – ANALYTICAL REPORT")
    lines.append(f"  Generated : {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")

    # ── 1. Volume ──────────────────────────────────────────────────────────
    h2("1. TRANSACTION VOLUME")
    total = fetch_one("SELECT COUNT(*) c, COALESCE(SUM(amount),0) s FROM all_transactions")
    fraud = fetch_one("""
        SELECT COUNT(*) c, COALESCE(SUM(a.amount),0) s
        FROM flagged_transactions f JOIN all_transactions a ON f.txn_ref=a.txn_ref
    """)
    validated = fetch_one("SELECT COUNT(*) c, COALESCE(SUM(amount),0) s FROM validated_ledger")

    row(f"Total transactions ingested : {int(total['c']):>8,}   ${float(total['s']):>14,.2f}")
    row(f"Flagged as fraud            : {int(fraud['c']):>8,}   ${float(fraud['s']):>14,.2f}")
    row(f"Validated (clean)           : {int(validated['c']):>8,}   ${float(validated['s']):>14,.2f}")

    # ── 2. Fraud by type ───────────────────────────────────────────────────
    h2("2. FRAUD BY TYPE")
    for r in _q("SELECT fraud_type, COUNT(*) c FROM flagged_transactions GROUP BY fraud_type"):
        row(f"{r['fraud_type']:<25} {r['c']:>6} alerts")

    # ── 3. Fraud by merchant ───────────────────────────────────────────────
    h2("3. FRAUD ATTEMPTS BY MERCHANT CATEGORY")
    row(f"{'Category':<22} {'Count':>6}  {'Amount':>14}")
    for r in _q("""
        SELECT a.merchant_category,
               COUNT(*) c, COALESCE(SUM(a.amount),0) s
        FROM   flagged_transactions f
        JOIN   all_transactions a ON f.txn_ref=a.txn_ref
        GROUP  BY a.merchant_category ORDER BY c DESC
    """):
        row(f"{r['merchant_category']:<22} {r['c']:>6}  ${float(r['s']):>13,.2f}")

    # ── 4. Fraud by country ────────────────────────────────────────────────
    h2("4. FRAUD ATTEMPTS BY COUNTRY")
    row(f"{'Country':<10} {'Count':>6}  {'Amount':>14}")
    for r in _q("""
        SELECT a.country,
               COUNT(*) c, COALESCE(SUM(a.amount),0) s
        FROM   flagged_transactions f
        JOIN   all_transactions a ON f.txn_ref=a.txn_ref
        GROUP  BY a.country ORDER BY c DESC
    """):
        row(f"{r['country']:<10} {r['c']:>6}  ${float(r['s']):>13,.2f}")

    # ── 5. Top flagged accounts ────────────────────────────────────────────
    h2("5. TOP 10 FLAGGED ACCOUNTS")
    row(f"{'Account ID (truncated)':<38} {'Alerts':>6}")
    for r in _q("""
        SELECT account_id, COUNT(*) c FROM flagged_transactions
        GROUP BY account_id ORDER BY c DESC LIMIT 10
    """):
        row(f"{r['account_id']:<38} {r['c']:>6}")

    # ── 6. Reconciliation ─────────────────────────────────────────────────
    h2("6. LATEST RECONCILIATION")
    rec = fetch_one(
        "SELECT * FROM reconciliation_log ORDER BY run_id DESC LIMIT 1"
    )
    if rec:
        row(f"Run timestamp    : {rec['run_ts']}")
        row(f"Total ingested   : ${float(rec['total_ingested']):,.2f}")
        row(f"Fraud amount     : ${float(rec['fraud_amount']):,.2f}")
        row(f"Validated amount : ${float(rec['validated_amt']):,.2f}")
        row(f"Status           : {rec['status']}")
    else:
        row("No reconciliation runs found.")

    h1("END OF REPORT")

    with open(path, "w") as fh:
        fh.write("\n".join(lines))

    print(f"[report] Full report saved → {path}")
    return path


if __name__ == "__main__":
    generate_full_report()
