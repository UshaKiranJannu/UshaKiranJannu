-- stg_raw_transactions.sql
-- Cleans the raw transactions table written by Spark.
-- Works on both DuckDB (dev) and Snowflake (prod).
-- The only difference is the timestamp function — handled via a macro.

{{ config(materialized='view') }}

SELECT
    transaction_id,
    account_id,
    TRIM(customer_name)                         AS customer_name,
    LOWER(TRIM(transaction_type))               AS transaction_type,
    amount,
    UPPER(COALESCE(currency, 'USD'))            AS currency,
    merchant,
    LOWER(TRIM(merchant_category))              AS merchant_category,
    location,
    {{ epoch_ms_to_timestamp('timestamp') }}    AS transaction_at,
    COALESCE(is_anomalous, FALSE)               AS is_anomalous,
    COALESCE(fraud_score, 0)                    AS fraud_score,
    fraud_reasons,
    {{ epoch_ms_to_timestamp('alert_timestamp') }} AS alerted_at
FROM {{ source('transactions', 'raw_transactions') }}
WHERE transaction_id IS NOT NULL
  AND account_id     IS NOT NULL
  AND amount         IS NOT NULL
