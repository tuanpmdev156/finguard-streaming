-- Point-in-time card status history built from snap_cards (SCD2).
-- Each row = one version of a card, valid in [valid_from, valid_to).
-- The first version of every card is back-dated to 1900-01-01 so that
-- transactions that happened before the snapshot started still match exactly one row.

WITH snap AS (
    SELECT * FROM {{ ref('snap_cards') }}
)

SELECT
    card_id,
    credit_limit,
    is_active,
    expiry_date,
    dbt_valid_from AS snapshot_valid_from,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY dbt_valid_from) = 1
            THEN TIMESTAMP '1900-01-01 00:00:00+00'
        ELSE dbt_valid_from
    END AS valid_from,
    COALESCE(dbt_valid_to, TIMESTAMP '9999-12-31 23:59:59+00') AS valid_to,
    dbt_valid_to IS NULL AS is_current_version
FROM snap
