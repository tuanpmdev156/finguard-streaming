-- Quarantine view isolating corrupt or malformed streaming transactions

WITH raw_tx AS (
    SELECT * FROM {{ source('raw', 'ext_streaming_transactions') }}
),

typed_tx AS (
    SELECT
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(card_id AS STRING) AS card_id,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(merchant_id AS STRING) AS merchant_id,
        ROUND(CAST(amount AS FLOAT64), 2) AS amount,
        UPPER(TRIM(CAST(currency AS STRING))) AS currency,
        TRIM(CAST(transaction_channel AS STRING)) AS transaction_channel,
        UPPER(TRIM(CAST(transaction_country AS STRING))) AS transaction_country,
        TRIM(CAST(transaction_city AS STRING)) AS transaction_city,
        CAST(ip_address AS STRING) AS ip_address,
        CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
        UPPER(TRIM(COALESCE(CAST(injected_scenario AS STRING), 'UNKNOWN'))) AS injected_scenario,
        UPPER(TRIM(COALESCE(CAST(stream_fraud_reason AS STRING), 'CORRUPT_AMOUNT'))) AS stream_fraud_reason,
        UPPER(TRIM(COALESCE(CAST(recommended_action AS STRING), 'QUARANTINE_RECORD'))) AS recommended_action,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM raw_tx
)

SELECT
    *,
    DATE(transaction_timestamp) AS transaction_date,
    CURRENT_TIMESTAMP() AS quarantined_at,
    CASE
        WHEN transaction_id IS NULL OR card_id IS NULL OR customer_id IS NULL OR merchant_id IS NULL
            THEN 'MISSING_MANDATORY_KEY'
        WHEN amount IS NULL OR amount <= 0
            THEN 'CORRUPT_NON_POSITIVE_AMOUNT'
        WHEN COALESCE(currency, '') != 'USD'
            THEN 'INVALID_CURRENCY_CODE'
        ELSE 'OTHER_SCHEMA_VIOLATION'
    END AS quarantine_reason
FROM typed_tx
WHERE transaction_id IS NULL
   OR card_id IS NULL
   OR customer_id IS NULL
   OR merchant_id IS NULL
   OR amount IS NULL
   OR amount <= 0
   OR COALESCE(currency, '') != 'USD'

