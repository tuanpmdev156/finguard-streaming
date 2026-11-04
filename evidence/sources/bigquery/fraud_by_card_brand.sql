SELECT
    COALESCE(NULLIF(card_brand, ''), 'Other') AS card_brand,
    SUM(total_transactions) AS total_transactions,
    SUM(fraud_transactions) AS fraud_transactions,
    ROUND(SAFE_DIVIDE(SUM(fraud_transactions), SUM(total_transactions)), 4) AS fraud_rate_pct,
    ROUND(SUM(total_amount), 2) AS total_amount,
    ROUND(SUM(fraud_exposure_amount), 2) AS fraud_exposure_amount
FROM `finguard_production.mart_fraud_risk_summary`
GROUP BY card_brand
ORDER BY fraud_rate_pct DESC

