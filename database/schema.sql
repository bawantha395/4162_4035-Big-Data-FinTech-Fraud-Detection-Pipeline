-- BankSecure Pipeline – PostgreSQL Schema
-- Run once before starting the pipeline.

CREATE TABLE IF NOT EXISTS all_transactions (
    txn_ref           TEXT PRIMARY KEY,
    account_id        TEXT        NOT NULL,
    event_ts          TEXT        NOT NULL,
    merchant_category TEXT,
    amount            NUMERIC(14, 2),
    country           TEXT,
    channel           TEXT,
    currency          TEXT        DEFAULT 'USD',
    proc_ts           TIMESTAMP   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS flagged_transactions (
    flag_id        SERIAL PRIMARY KEY,
    txn_ref        TEXT,
    account_id     TEXT        NOT NULL,
    fraud_type     TEXT        NOT NULL,   -- HIGH_VALUE | IMPOSSIBLE_TRAVEL
    detection_ts   TIMESTAMP   DEFAULT NOW(),
    details        JSONB
);

CREATE TABLE IF NOT EXISTS validated_ledger (
    txn_ref           TEXT PRIMARY KEY,
    account_id        TEXT,
    event_ts          TEXT,
    merchant_category TEXT,
    amount            NUMERIC(14, 2),
    country           TEXT,
    channel           TEXT,
    currency          TEXT,
    proc_ts           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reconciliation_log (
    run_id         SERIAL PRIMARY KEY,
    run_ts         TIMESTAMP   DEFAULT NOW(),
    total_ingested NUMERIC(18, 2),
    fraud_amount   NUMERIC(18, 2),
    validated_amt  NUMERIC(18, 2),
    status         TEXT        -- PASS | FAIL
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_all_txn_account  ON all_transactions  (account_id);
CREATE INDEX IF NOT EXISTS idx_flagged_account  ON flagged_transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_flagged_type     ON flagged_transactions (fraud_type);
CREATE INDEX IF NOT EXISTS idx_validated_acct   ON validated_ledger   (account_id);
