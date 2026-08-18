-- stg_flagged_transactions.sql
-- Cleans the flagged transactions table (fraud_score > 0 only).

{{ config(materialized='view') }}

SELECT
    transaction_id,
    account_id,
    amount,
    LOWER(TRIM(transaction_type))               AS transaction_type,
    location,
    {{ epoch_ms_to_timestamp('timestamp') }}    AS transaction_at,
    fraud_score,
    fraud_reasons,
    {{ epoch_ms_to_timestamp('alert_timestamp') }} AS alerted_at
FROM {{ source('transactions', 'flagged_transactions') }}
WHERE transaction_id IS NOT NULL
  AND fraud_score    > 0
