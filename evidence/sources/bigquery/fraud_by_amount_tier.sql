SELECT
    amount_tier,
    CASE amount_tier
        WHEN 'micro' THEN 1
        WHEN 'low' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'high' THEN 4
        WHEN 'critical' THEN 5
        ELSE 6
    END AS tier_order,
    SUM(total_transactions) AS total_transactions,
    SUM(fraud_transactions) AS fraud_transactions,
    ROUND(SAFE_DIVIDE(SUM(fraud_transactions), SUM(total_transactions)), 4) AS fraud_rate_pct,
    ROUND(SUM(total_amount), 2) AS total_amount,
    ROUND(SUM(fraud_exposure_amount), 2) AS fraud_exposure_amount
FROM `finguard_production.mart_fraud_risk_summary`
GROUP BY amount_tier, tier_order
ORDER BY tier_order

