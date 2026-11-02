-- Staging view for customer reference profiles

WITH raw_customers AS (
    SELECT * FROM {{ source('raw', 'ext_customers') }}
)

SELECT
    CAST(customer_id AS STRING) AS customer_id,
    TRIM(CAST(full_name AS STRING)) AS full_name,
    LOWER(TRIM(CAST(email AS STRING))) AS email,
    TRIM(CAST(phone AS STRING)) AS phone,
    UPPER(TRIM(CAST(home_country AS STRING))) AS home_country,
    TRIM(CAST(home_city AS STRING)) AS home_city,
    UPPER(TRIM(COALESCE(CAST(risk_profile AS STRING), 'LOW'))) AS risk_profile,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM raw_customers
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY updated_at DESC, created_at DESC
) = 1

