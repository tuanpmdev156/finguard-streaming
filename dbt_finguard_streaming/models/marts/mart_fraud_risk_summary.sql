-- Pre-aggregated multi-dimensional fraud risk KPIs for Evidence.dev BI Dashboard
{{
    config(
        materialized='table',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["transaction_channel", "card_brand", "merchant_category"]
    )
}}

WITH fct AS (
    SELECT * FROM {{ ref('fct_streaming_transactions') }}
)

SELECT
    -- Dimensional Grains
    transaction_date,
    transaction_hour,
    transaction_channel,
    card_brand,
    card_type,
    merchant_category,
    mcc_code,
    transaction_country,
    amount_tier,

    -- Volume Metrics
    COUNT(1) AS total_transactions,
    COUNTIF(is_fraud = 1) AS fraud_transactions,
    COUNTIF(is_fraud = 0) AS legit_transactions,

    -- Fraud Rate (%) bounded in [0, 100]
    ROUND(SAFE_DIVIDE(COUNTIF(is_fraud = 1), COUNT(1)) * 100, 4) AS fraud_rate,

    -- Financial Exposure Metrics (USD)
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2) AS fraud_exposure_amount,
    ROUND(AVG(amount), 2) AS avg_transaction_amount,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN amount ELSE NULL END), 2) AS avg_fraud_amount,

    -- Specific Rule Breakdown Counts
    COUNTIF(is_velocity_attack) AS velocity_attack_count,
    COUNTIF(is_high_amount_spike) AS high_amount_spike_count,
    COUNTIF(is_geo_mismatch) AS geo_mismatch_count,
    COUNTIF(is_compromised_card) AS compromised_card_count
FROM fct
GROUP BY
    transaction_date,
    transaction_hour,
    transaction_channel,
    card_brand,
    card_type,
    merchant_category,
    mcc_code,
    transaction_country,
    amount_tier

