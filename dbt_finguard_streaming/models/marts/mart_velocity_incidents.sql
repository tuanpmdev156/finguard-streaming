-- Specialized SecOps audit mart for Velocity Attacks and Compromised Card incidents
{{
    config(
        materialized='table',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["card_brand", "stream_fraud_reason", "recommended_action"]
    )
}}

WITH fct AS (
    SELECT * FROM {{ ref('fct_streaming_transactions') }}
    WHERE is_fraud = 1
)

SELECT
    transaction_id,
    transaction_timestamp,
    transaction_date,
    transaction_hour,
    card_id,
    card_number_masked,
    card_brand,
    card_type,
    is_card_active,
    customer_id,
    customer_name,
    customer_home_country,
    customer_risk_profile,
    merchant_id,
    merchant_name,
    merchant_category,
    mcc_code,
    transaction_channel,
    transaction_country,
    transaction_city,
    ip_address,
    amount,
    amount_tier,
    credit_limit,
    batch_card_tx_count,
    stream_fraud_reason,
    recommended_action,
    risk_score,
    risk_tier,
    is_velocity_attack,
    is_high_amount_spike,
    is_geo_mismatch,
    is_compromised_card,
    ingested_at
FROM fct

