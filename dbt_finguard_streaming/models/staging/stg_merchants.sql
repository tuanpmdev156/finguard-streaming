-- Staging view for merchants reference data

WITH raw_merchants AS (
    SELECT * FROM {{ source('raw', 'ext_merchants') }}
)

SELECT
    CAST(merchant_id AS STRING) AS merchant_id,
    TRIM(CAST(merchant_name AS STRING)) AS merchant_name,
    TRIM(CAST(mcc_code AS STRING)) AS mcc_code,
    TRIM(CAST(merchant_category AS STRING)) AS merchant_category,
    UPPER(TRIM(CAST(country AS STRING))) AS merchant_country,
    TRIM(CAST(city AS STRING)) AS merchant_city,
    COALESCE(CAST(is_high_risk_mcc AS BOOL), FALSE) AS is_high_risk_mcc,
    CAST(created_at AS TIMESTAMP) AS created_at
FROM raw_merchants
WHERE merchant_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY merchant_id
    ORDER BY created_at DESC
) = 1

