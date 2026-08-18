"""
snowflake_writer.py
--------------------
Utility helpers for writing DataFrames to Snowflake.
Also contains the DDL strings needed to create the target tables the first time.

Can be run directly to initialise the Snowflake schema:
    python pipeline/snowflake_writer.py --init-schema
"""

from __future__ import annotations

import argparse

import snowflake.connector
from loguru import logger

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.pipeline_config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_RAW_TABLE,
    SNOWFLAKE_FLAGGED_TABLE,
)

# ─── DDL ─────────────────────────────────────────────────────────────────────

DDL_RAW_TRANSACTIONS = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_RAW_TABLE} (
    transaction_id      VARCHAR(36)     NOT NULL,
    account_id          VARCHAR(20)     NOT NULL,
    customer_name       VARCHAR(200),
    transaction_type    VARCHAR(20)     NOT NULL,
    amount              FLOAT           NOT NULL,
    currency            VARCHAR(10),
    merchant            VARCHAR(200),
    merchant_category   VARCHAR(50),
    location            VARCHAR(100),
    timestamp           BIGINT          NOT NULL,
    is_anomalous        BOOLEAN,
    fraud_score         INTEGER,
    fraud_reasons       VARCHAR(200),
    alert_timestamp     BIGINT,
    PRIMARY KEY (transaction_id)
);
"""

DDL_FLAGGED_TRANSACTIONS = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_FLAGGED_TABLE} (
    transaction_id      VARCHAR(36)     NOT NULL,
    account_id          VARCHAR(20)     NOT NULL,
    amount              FLOAT           NOT NULL,
    transaction_type    VARCHAR(20)     NOT NULL,
    location            VARCHAR(100),
    timestamp           BIGINT          NOT NULL,
    fraud_score         INTEGER         NOT NULL,
    fraud_reasons       VARCHAR(200),
    alert_timestamp     BIGINT          NOT NULL,
    PRIMARY KEY (transaction_id)
);
"""

DDL_WINDOW_AGGREGATIONS = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.WINDOW_AGGREGATIONS (
    account_id          VARCHAR(20)     NOT NULL,
    window_start        BIGINT          NOT NULL,
    window_end          BIGINT          NOT NULL,
    txn_count_in_window INTEGER         NOT NULL,
    total_amount        FLOAT           NOT NULL,
    max_amount          FLOAT           NOT NULL,
    unique_locations    INTEGER         NOT NULL,
    unique_merchants    INTEGER         NOT NULL,
    PRIMARY KEY (account_id, window_start)
);
"""


# ─── Connection helper ────────────────────────────────────────────────────────

def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )


# ─── Schema initialisation ────────────────────────────────────────────────────

def init_schema() -> None:
    """
    Create database, schema, and all target tables if they don't exist yet.
    Safe to run multiple times (uses IF NOT EXISTS everywhere).
    """
    conn = get_connection()
    cur  = conn.cursor()

    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        for ddl, name in [
            (DDL_RAW_TRANSACTIONS,    SNOWFLAKE_RAW_TABLE),
            (DDL_FLAGGED_TRANSACTIONS, SNOWFLAKE_FLAGGED_TABLE),
            (DDL_WINDOW_AGGREGATIONS,  "WINDOW_AGGREGATIONS"),
        ]:
            cur.execute(ddl)
            logger.info("Table ready: {}.{}.{}", SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, name)

        logger.info("Snowflake schema initialised successfully.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snowflake schema management")
    parser.add_argument("--init-schema", action="store_true", help="Create all tables")
    args = parser.parse_args()

    if args.init_schema:
        init_schema()
    else:
        parser.print_help()
