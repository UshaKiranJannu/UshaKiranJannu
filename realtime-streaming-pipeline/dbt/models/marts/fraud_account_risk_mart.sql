-- fraud_account_risk_mart.sql
-- One row per account with full risk profile.
-- BI tools (Tableau / Power BI) query this table directly.
-- post_hook CLUSTER BY only applies on Snowflake — skipped on DuckDB.

{{ config(
    materialized='table',
    post_hook="{% if target.type == 'snowflake' %}ALTER TABLE {{ this }} CLUSTER BY (is_high_risk, last_flagged_at){% endif %}"
) }}

SELECT
    r.account_id,
    r.total_flagged_txns,
    r.total_flagged_amount,
    r.max_fraud_score,
    r.avg_fraud_score,
    r.first_flagged_at,
    r.last_flagged_at,
    r.distinct_fraud_reason_combos,
    r.is_high_risk,
    t.total_txns,
    t.total_txn_amount,
    ROUND(
        100.0 * r.total_flagged_txns / NULLIF(t.total_txns, 0),
        2
    )                           AS fraud_rate_pct,
    current_timestamp           AS mart_refreshed_at
FROM {{ ref('int_account_risk_summary') }} r
LEFT JOIN (
    SELECT
        account_id,
        COUNT(*)    AS total_txns,
        SUM(amount) AS total_txn_amount
    FROM {{ ref('stg_raw_transactions') }}
    GROUP BY account_id
) t USING (account_id)
