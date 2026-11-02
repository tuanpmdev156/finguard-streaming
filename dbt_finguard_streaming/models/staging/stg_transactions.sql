-- Staging view for clean, deduplicated streaming transactions

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
        {{ classify_amount('CAST(amount AS FLOAT64)') }} AS amount_tier,
        UPPER(TRIM(COALESCE(CAST(currency AS STRING), 'USD'))) AS currency,
        TRIM(CAST(transaction_channel AS STRING)) AS transaction_channel,
        UPPER(TRIM(CAST(transaction_country AS STRING))) AS transaction_country,
        TRIM(CAST(transaction_city AS STRING)) AS transaction_city,
        CAST(ip_address AS STRING) AS ip_address,
        CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
        UPPER(TRIM(COALESCE(CAST(injected_scenario AS STRING), 'NORMAL'))) AS injected_scenario,
        COALESCE(CAST(batch_card_tx_count AS INT64), 1) AS batch_card_tx_count,
        COALESCE(CAST(stream_fraud_flag AS BOOL), FALSE) AS stream_fraud_flag,
        UPPER(TRIM(COALESCE(CAST(stream_fraud_reason AS STRING), 'CLEAN'))) AS stream_fraud_reason,
        UPPER(TRIM(COALESCE(CAST(recommended_action AS STRING), 'APPROVE'))) AS recommended_action,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM raw_tx
)

SELECT
    transaction_id,
    card_id,
    customer_id,
    merchant_id,
    amount,
    amount_tier,
    currency,
    transaction_channel,
    transaction_country,
    transaction_city,
    ip_address,
    transaction_timestamp,
    DATE(transaction_timestamp) AS transaction_date,
    EXTRACT(HOUR FROM transaction_timestamp) AS transaction_hour,
    EXTRACT(DAYOFWEEK FROM transaction_timestamp) AS day_of_week,
    injected_scenario,
    batch_card_tx_count,
    stream_fraud_flag,
    stream_fraud_reason,
    recommended_action,
    ingested_at
FROM typed_tx
WHERE transaction_id IS NOT NULL
  AND card_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND merchant_id IS NOT NULL
  AND amount > 0
  AND currency = 'USD'
-- Deduplicate records caused by at-least-once delivery in Kafka/Redpanda & Spark Streaming
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id
    ORDER BY ingested_at DESC, transaction_timestamp DESC
) = 1

