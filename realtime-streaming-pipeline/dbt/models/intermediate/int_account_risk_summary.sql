-- int_account_risk_summary.sql
-- Per-account risk summary built from the staging flagged transactions view.
-- Used downstream by the fraud_account_risk_mart.

{{ config(materialized='ephemeral') }}

SELECT
    account_id,
    COUNT(*)                                              AS total_flagged_txns,
    SUM(amount)                                           AS total_flagged_amount,
    MAX(fraud_score)                                      AS max_fraud_score,
    ROUND(AVG(fraud_score), 2)                            AS avg_fraud_score,
    MIN(transaction_at)                                   AS first_flagged_at,
    MAX(transaction_at)                                   AS last_flagged_at,
    -- How many distinct fraud rule types triggered across all alerts
    COUNT(DISTINCT fraud_reasons)                         AS distinct_fraud_reason_combos,
    -- Accounts with a max score >= 66 are considered high-risk
    CASE WHEN MAX(fraud_score) >= 66 THEN TRUE ELSE FALSE END AS is_high_risk
FROM {{ ref('stg_flagged_transactions') }}
GROUP BY account_id
