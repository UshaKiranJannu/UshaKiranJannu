"""
fraud_detector.py

Rule-based fraud scoring that runs inside Spark's foreachBatch on each micro-batch.

I kept this deliberately simple — three heuristic rules, each worth 33 points, cap at 99.
The intent is to demonstrate the pattern cleanly, not to build a production ML model.
In a real setup you'd want a model here (or at least a lookup against a trained
feature store), but rule-based is much easier to explain, test, and tune.

Rules:
  1. large_amount   – amount > $10,000
  2. unusual_hour   – transaction between midnight and 4 AM
  3. high_velocity  – same account has >= 5 transactions in the current 5-min window

All three can fire simultaneously — a $25k cash withdrawal at 2 AM from an account
that's already sent 6 transactions this window gets a score of 99.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.pipeline_config import (
    FRAUD_LARGE_AMOUNT_THRESHOLD,
    FRAUD_UNUSUAL_HOUR_START,
    FRAUD_UNUSUAL_HOUR_END,
    FRAUD_VELOCITY_COUNT_THRESHOLD,
)


# ─── Rule flag columns ────────────────────────────────────────────────────────

def add_rule_flags(df: DataFrame) -> DataFrame:
    """Add boolean flag columns for each fraud rule.

    Expects columns: amount, timestamp (epoch ms), txn_count_in_window (optional).
    If txn_count_in_window is absent we default it to 1 — this handles the case
    where the window aggregation hasn't been joined in yet (e.g. first micro-batch).
    """
    # timestamp is stored as epoch-ms — dividing by 1000 gives Spark a Unix timestamp
    # it can convert to a proper datetime, from which we pull the hour
    hour_col = F.hour(F.from_unixtime(F.col("timestamp") / 1000))

    df = df.withColumn(
        "flag_large_amount",
        F.col("amount") > F.lit(FRAUD_LARGE_AMOUNT_THRESHOLD),
    ).withColumn(
        "flag_unusual_hour",
        hour_col.between(FRAUD_UNUSUAL_HOUR_START, FRAUD_UNUSUAL_HOUR_END),
    )

    # If txn_count_in_window doesn't exist yet, add it as 1.
    # This lets fraud_detector run standalone (unit tests, replays) without
    # requiring the window aggregation step to have run first.
    if "txn_count_in_window" not in df.columns:
        df = df.withColumn("txn_count_in_window", F.lit(1))

    df = df.withColumn(
        "flag_high_velocity",
        F.col("txn_count_in_window") >= F.lit(FRAUD_VELOCITY_COUNT_THRESHOLD),
    )

    return df


# ─── Score & reason aggregation ──────────────────────────────────────────────

def compute_fraud_score(df: DataFrame) -> DataFrame:
    """Compute fraud_score (0–99) and fraud_reasons from the rule flag columns.

    Each fired rule contributes 33 points. The cap at 99 (not 100) is intentional —
    99 reads as "very high confidence" without implying absolute certainty.
    fraud_reasons is a comma-separated string of rule names so downstream consumers
    (dashboards, alert emails) can explain the score without re-running the logic.
    """
    score_col = (
        F.col("flag_large_amount").cast(IntegerType()) * F.lit(33)
        + F.col("flag_unusual_hour").cast(IntegerType()) * F.lit(33)
        + F.col("flag_high_velocity").cast(IntegerType()) * F.lit(33)
    )

    # concat_ws skips nulls, so untriggered rules (which return null from F.when)
    # are cleanly omitted from the reasons string
    reasons_col = F.concat_ws(
        ",",
        F.when(F.col("flag_large_amount"), F.lit("large_amount")),
        F.when(F.col("flag_unusual_hour"), F.lit("unusual_hour")),
        F.when(F.col("flag_high_velocity"), F.lit("high_velocity")),
    ).cast(StringType())

    return df.withColumn(
        "fraud_score", F.least(score_col, F.lit(99))
    ).withColumn(
        "fraud_reasons", reasons_col
    )


# ─── Filter to only flagged records ──────────────────────────────────────────

def filter_fraud_alerts(df: DataFrame) -> DataFrame:
    """Keep only transactions where at least one fraud rule fired."""
    return df.filter(F.col("fraud_score") > 0)
