-- Singular test: Ensure fraud_rate percentage in mart_fraud_risk_summary is strictly within [0, 100]
SELECT
    transaction_date,
    transaction_channel,
    card_brand,
    fraud_rate
FROM {{ ref('mart_fraud_risk_summary') }}
WHERE fraud_rate < 0 OR fraud_rate > 100

