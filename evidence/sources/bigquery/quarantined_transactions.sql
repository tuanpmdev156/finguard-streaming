SELECT
    COALESCE(transaction_id, 'NULL_TX_ID') AS transaction_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', transaction_timestamp) AS transaction_time_utc,
    transaction_date,
    COALESCE(card_id, 'NULL_CARD') AS card_id,
    COALESCE(customer_id, 'NULL_CUST') AS customer_id,
    COALESCE(merchant_id, 'NULL_MERCH') AS merchant_id,
    amount,
    currency,
    transaction_channel,
    transaction_country,
    injected_scenario,
    stream_fraud_reason,
    recommended_action,
    quarantine_reason
FROM `finguard_staging.stg_quarantined_transactions`
ORDER BY transaction_timestamp DESC

