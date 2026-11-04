SELECT
    transaction_channel,
    SUM(total_transactions) AS total_transactions,
    SUM(fraud_transactions) AS fraud_transactions,
    ROUND(SAFE_DIVIDE(SUM(fraud_transactions), SUM(total_transactions)), 4) AS fraud_rate_pct,
    ROUND(SUM(fraud_exposure_amount), 2) AS fraud_exposure_amount
FROM `finguard_production.mart_fraud_risk_summary`
GROUP BY transaction_channel
ORDER BY fraud_rate_pct DESC, fraud_exposure_amount DESC
LIMIT 1

