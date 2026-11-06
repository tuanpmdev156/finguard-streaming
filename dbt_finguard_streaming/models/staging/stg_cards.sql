-- Staging view for payment cards reference data

WITH raw_cards AS (
    SELECT * FROM {{ source('raw', 'ext_cards') }}
)

SELECT
    CAST(card_id AS STRING) AS card_id,
    CAST(customer_id AS STRING) AS customer_id,
    TRIM(CAST(card_number_masked AS STRING)) AS card_number_masked,
    TRIM(CAST(card_brand AS STRING)) AS card_brand,
    UPPER(TRIM(CAST(card_type AS STRING))) AS card_type,
    ROUND(CAST(credit_limit AS FLOAT64), 2) AS credit_limit,
    CAST(is_active AS BOOL) AS is_active,
    CAST(expiry_date AS DATE) AS expiry_date,
    CAST(issued_at AS TIMESTAMP) AS issued_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM raw_cards
WHERE card_id IS NOT NULL
  AND customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY card_id
    ORDER BY updated_at DESC, issued_at DESC
) = 1

