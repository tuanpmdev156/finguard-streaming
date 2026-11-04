SELECT
    transaction_date,
    transaction_hour,
    transaction_channel,
    card_brand,
    card_type,
    merchant_category,
    mcc_code,
    transaction_country,
    amount_tier,
    total_transactions,
    fraud_transactions,
    legit_transactions,
    fraud_rate,
    total_amount,
    fraud_exposure_amount,
    avg_transaction_amount,
    avg_fraud_amount,
    velocity_attack_count,
    high_amount_spike_count,
    geo_mismatch_count,
    compromised_card_count
FROM `finguard_production.mart_fraud_risk_summary`

