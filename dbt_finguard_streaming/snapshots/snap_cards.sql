-- SCD Type 2 snapshot of card status (is_active / credit_limit / expiry_date).
-- Strategy 'timestamp' on cards.updated_at (maintained by PostgreSQL trigger
-- trg_cards_update_timestamp) => dbt_valid_from = actual change time in OLTP,
-- not the time the snapshot job ran.
-- Used by stg_cards_history for point-in-time joins in fct_streaming_transactions.

{% snapshot snap_cards %}

{{
    config(
        target_schema='finguard_snapshots',
        unique_key='card_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    card_id,
    customer_id,
    credit_limit,
    is_active,
    expiry_date,
    updated_at
FROM {{ ref('stg_cards') }}

{% endsnapshot %}
