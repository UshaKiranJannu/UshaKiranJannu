-- int_hourly_transaction_volume.sql
-- Aggregates all transactions by hour for volume trending.
-- Downstream mart uses this to spot anomalous spikes.

{{ config(materialized='ephemeral') }}

SELECT
    DATE_TRUNC('hour', transaction_at)  AS txn_hour,
    transaction_type,
    merchant_category,
    COUNT(*)                            AS txn_count,
    SUM(amount)                         AS total_amount,
    ROUND(AVG(amount), 2)               AS avg_amount,
    MAX(amount)                         AS max_amount,
    SUM(CASE WHEN fraud_score > 0 THEN 1 ELSE 0 END) AS flagged_count
FROM {{ ref('stg_raw_transactions') }}
GROUP BY 1, 2, 3
