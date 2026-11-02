-- Singular test: Ensure no transaction with amount <= 0 enters the production Fact table
SELECT
    transaction_id,
    amount
FROM {{ ref('fct_streaming_transactions') }}
WHERE amount <= 0

