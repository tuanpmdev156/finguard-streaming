SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', transaction_timestamp) AS transaction_time_utc,
    card_number_masked,
    card_brand,
    customer_name,
    transaction_channel,
    transaction_country,
    amount,
    batch_card_tx_count,
    stream_fraud_reason,
    recommended_action,
    risk_score,
    risk_tier
FROM `finguard_production.mart_velocity_incidents`
WHERE transaction_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY transaction_timestamp DESC, risk_score DESC
LIMIT 5000


