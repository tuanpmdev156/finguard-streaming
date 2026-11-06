-- Migration 001: add updated_at + trigger to cards (fix_fraud_rate_100pct.md - Phase 1 / RC4)
-- Idempotent: safe to run multiple times on an existing database (Cloud SQL).
-- Requires fn_set_updated_at() from init_tables.sql (re-created here for safety).

BEGIN;

ALTER TABLE cards ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE cards ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

-- Backfill: the real change time of existing rows is unknown -> use issued_at
UPDATE cards SET updated_at = issued_at WHERE updated_at IS NULL;

CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_cards_update_timestamp ON cards;
CREATE TRIGGER trg_cards_update_timestamp
BEFORE UPDATE ON cards
FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

COMMIT;
