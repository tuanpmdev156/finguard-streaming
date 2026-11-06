CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(36) PRIMARY KEY,
    full_name VARCHAR(120) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    phone VARCHAR(30),
    home_country VARCHAR(5) NOT NULL,
    home_city VARCHAR(80) NOT NULL,
    risk_profile VARCHAR(20) DEFAULT 'LOW' CHECK (risk_profile IN ('LOW', 'MEDIUM', 'HIGH')),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cards (
    card_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(36) NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    card_number_masked VARCHAR(25) NOT NULL,
    card_brand VARCHAR(20) NOT NULL CHECK (card_brand IN ('Visa', 'Mastercard', 'Amex')),
    card_type VARCHAR(20) NOT NULL CHECK (card_type IN ('CREDIT', 'DEBIT', 'PREPAID')),
    credit_limit NUMERIC(12, 2) NOT NULL CHECK (credit_limit > 0),
    is_active BOOLEAN DEFAULT TRUE,
    expiry_date DATE NOT NULL,
    issued_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id VARCHAR(36) PRIMARY KEY,
    merchant_name VARCHAR(150) NOT NULL,
    mcc_code VARCHAR(10) NOT NULL,
    merchant_category VARCHAR(80) NOT NULL,
    country VARCHAR(5) NOT NULL,
    city VARCHAR(80) NOT NULL,
    is_high_risk_mcc BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cards_customer ON cards(customer_id);
CREATE INDEX IF NOT EXISTS idx_cards_active ON cards(is_active);
CREATE INDEX IF NOT EXISTS idx_merchants_mcc ON merchants(mcc_code);

CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_customers_update_timestamp ON customers;
CREATE TRIGGER trg_customers_update_timestamp
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

DROP TRIGGER IF EXISTS trg_cards_update_timestamp ON cards;
CREATE TRIGGER trg_cards_update_timestamp
BEFORE UPDATE ON cards
FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();