-- transaction_volume_mart.sql
-- Hourly volume mart for operational dashboards.

{{ config(materialized='table') }}

SELECT
    txn_hour,
    transaction_type,
    merchant_category,
    txn_count,
    total_amount,
    avg_amount,
    max_amount,
    flagged_count,
    ROUND(100.0 * flagged_count / NULLIF(txn_count, 0), 2) AS flagged_pct,
    current_timestamp AS mart_refreshed_at
FROM {{ ref('int_hourly_transaction_volume') }}
ORDER BY txn_hour DESC, txn_count DESC
