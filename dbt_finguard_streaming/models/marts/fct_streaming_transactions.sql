{{
    config(
        materialized='table',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["transaction_channel", "card_brand", "is_fraud"]
    )
}}

WITH tx AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- Static card attributes (brand, type, masked number) – do not change over time
cards AS (
    SELECT * FROM {{ ref('stg_cards') }}
),

-- Point-in-time card status (is_active, credit_limit, expiry_date) from SCD2 snapshot
cards_history AS (
    SELECT * FROM {{ ref('stg_cards_history') }}
),

merchants AS (
    SELECT * FROM {{ ref('stg_merchants') }}
),

enriched AS (
    SELECT
        t.transaction_id,
        t.transaction_timestamp,
        t.transaction_date,
        t.transaction_hour,
        t.day_of_week,

        -- Foreign Keys & Entity Attributes
        t.customer_id,
        COALESCE(cu.full_name, 'Unknown Customer') AS customer_name,
        COALESCE(cu.home_country, 'UNK') AS customer_home_country,
        COALESCE(cu.home_city, 'Unknown') AS customer_home_city,
        COALESCE(cu.risk_profile, 'LOW') AS customer_risk_profile,

        t.card_id,
        COALESCE(ca.card_number_masked, '****') AS card_number_masked,
        COALESCE(ca.card_brand, 'Visa') AS card_brand,
        COALESCE(ca.card_type, 'CREDIT') AS card_type,
        -- Card status as of transaction time (point-in-time)
        COALESCE(ch.credit_limit, 0.0) AS credit_limit,
        COALESCE(ch.is_active, TRUE) AS is_card_active,
        ch.expiry_date AS card_expiry_date,

        t.merchant_id,
        COALESCE(m.merchant_name, 'Unknown Merchant') AS merchant_name,
        COALESCE(m.mcc_code, '0000') AS mcc_code,
        COALESCE(m.merchant_category, 'General') AS merchant_category,
        COALESCE(m.is_high_risk_mcc, FALSE) AS is_high_risk_mcc,

        -- Transaction Details
        t.amount,
        t.amount_tier,
        t.currency,
        t.transaction_channel,
        t.transaction_country,
        t.transaction_city,
        t.ip_address,
        t.batch_card_tx_count,
        t.injected_scenario,
        t.stream_fraud_flag,
        t.stream_fraud_reason,
        t.recommended_action,
        t.ingested_at,

        -- Multi-Layer Warehouse Fraud Rule Evaluations
        CASE
            WHEN t.batch_card_tx_count >= 4 OR t.stream_fraud_reason = 'VELOCITY_ATTACK' THEN TRUE
            ELSE FALSE
        END AS is_velocity_attack,

        CASE
            WHEN t.amount > 5000.0
              OR (ch.credit_limit > 0 AND t.amount > ch.credit_limit * 0.90)
              OR t.stream_fraud_reason = 'HIGH_AMOUNT_SPIKE'
            THEN TRUE
            ELSE FALSE
        END AS is_high_amount_spike,

        CASE
            WHEN (t.transaction_channel IN ('POS', 'ATM') AND cu.home_country IS NOT NULL AND t.transaction_country != cu.home_country)
              OR t.stream_fraud_reason = 'GEO_MISMATCH'
            THEN TRUE
            ELSE FALSE
        END AS is_geo_mismatch,

        CASE
            WHEN ch.is_active = FALSE
              OR (ch.expiry_date IS NOT NULL AND ch.expiry_date < t.transaction_date)
              OR t.stream_fraud_reason = 'COMPROMISED_CARD'
            THEN TRUE
            ELSE FALSE
        END AS is_compromised_card
    FROM tx AS t
    LEFT JOIN customers AS cu
        ON t.customer_id = cu.customer_id
    LEFT JOIN cards AS ca
        ON t.card_id = ca.card_id
    LEFT JOIN cards_history AS ch
        ON t.card_id = ch.card_id
       AND t.transaction_timestamp >= ch.valid_from
       AND t.transaction_timestamp <  ch.valid_to
    LEFT JOIN merchants AS m
        ON t.merchant_id = m.merchant_id
),

scored AS (
    SELECT
        *,
        LEAST(
            100,
            (CASE WHEN is_velocity_attack THEN 55 ELSE 0 END)
            + (CASE WHEN is_compromised_card THEN 50 ELSE 0 END)
            + (CASE WHEN is_high_amount_spike THEN 45 ELSE 0 END)
            + (CASE WHEN is_geo_mismatch THEN 40 ELSE 0 END)
            + (CASE WHEN is_high_risk_mcc THEN 15 ELSE 0 END)
            + (CASE WHEN customer_risk_profile = 'HIGH' THEN 15 WHEN customer_risk_profile = 'MEDIUM' THEN 5 ELSE 0 END)
        ) AS risk_score
    FROM enriched
)

SELECT
    *,
    CASE
        WHEN risk_score >= 50 THEN 'CRITICAL'
        WHEN risk_score >= 40 THEN 'HIGH'
        WHEN risk_score >= 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,
    CASE
        WHEN stream_fraud_flag
          OR is_velocity_attack
          OR is_high_amount_spike
          OR is_geo_mismatch
          OR is_compromised_card
        THEN 1
        ELSE 0
    END AS is_fraud
FROM scored

