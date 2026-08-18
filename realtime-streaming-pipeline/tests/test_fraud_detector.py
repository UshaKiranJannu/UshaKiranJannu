"""
test_fraud_detector.py

Tests for the fraud detection logic. I'm using a local SparkSession so these
run without any external dependencies — no Kafka, no Snowflake needed.

The rule logic lives in fraud_detector.py. Three rules, each worth 33 points:
  - large_amount   (> $10k)
  - unusual_hour   (midnight to 4 AM)
  - high_velocity  (>= 5 txns from the same account in a 5-min window)

I verify each rule in isolation first, then make sure score composition and
the reasons string work correctly when multiple rules fire together.
"""

import pytest
from pyspark.sql import Row

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline.fraud_detector import add_rule_flags, compute_fraud_score, filter_fraud_alerts
from config.pipeline_config import (
    FRAUD_LARGE_AMOUNT_THRESHOLD,
    FRAUD_VELOCITY_COUNT_THRESHOLD,
    FRAUD_UNUSUAL_HOUR_START,
    FRAUD_UNUSUAL_HOUR_END,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def ts(hour: int) -> int:
    """Build an epoch-millisecond timestamp for today at a specific hour.
    Keeping this as a simple helper rather than a fixture — it's just math."""
    from datetime import datetime
    dt = datetime.now().replace(hour=hour, minute=0, second=0, microsecond=0)
    return int(dt.timestamp() * 1000)


def make_txn(spark, *, amount=100.0, hour=10, velocity=1, txn_id="T0"):
    """One-liner to build a single-row DataFrame for a test transaction.
    Defaults are all safe (low amount, business hours, velocity=1).
    Override whatever field you're testing — keeps test bodies short.
    """
    return spark.createDataFrame([Row(
        transaction_id=txn_id,
        account_id="ACC-TEST",
        amount=float(amount),
        timestamp=ts(hour),
        txn_count_in_window=velocity,
    )])


# ── add_rule_flags ────────────────────────────────────────────────────────────

class TestRuleFlags:
    """Testing each rule in isolation — I want failures to point at exactly one rule."""

    # large_amount rule
    def test_large_amount_fires_above_threshold(self, spark):
        row = add_rule_flags(make_txn(spark, amount=FRAUD_LARGE_AMOUNT_THRESHOLD + 0.01)).collect()[0]
        assert row["flag_large_amount"] is True

    def test_large_amount_silent_below_threshold(self, spark):
        row = add_rule_flags(make_txn(spark, amount=FRAUD_LARGE_AMOUNT_THRESHOLD - 0.01)).collect()[0]
        assert row["flag_large_amount"] is False

    def test_large_amount_silent_at_exact_threshold(self, spark):
        # The rule is strictly >, not >=. $10,000.00 exactly should NOT flag.
        row = add_rule_flags(make_txn(spark, amount=FRAUD_LARGE_AMOUNT_THRESHOLD)).collect()[0]
        assert row["flag_large_amount"] is False

    # unusual_hour rule — I had to think about this: does hour=4 flag or not?
    # FRAUD_UNUSUAL_HOUR_END=4 and we use .between() which is inclusive on both ends,
    # so 4 AM does flag. That matches the intent (early morning is suspicious).
    def test_unusual_hour_fires_at_midnight(self, spark):
        row = add_rule_flags(make_txn(spark, hour=FRAUD_UNUSUAL_HOUR_START)).collect()[0]
        assert row["flag_unusual_hour"] is True

    def test_unusual_hour_fires_at_4am(self, spark):
        row = add_rule_flags(make_txn(spark, hour=FRAUD_UNUSUAL_HOUR_END)).collect()[0]
        assert row["flag_unusual_hour"] is True

    def test_unusual_hour_silent_at_5am(self, spark):
        # 5 AM is outside the window — should be clean
        row = add_rule_flags(make_txn(spark, hour=FRAUD_UNUSUAL_HOUR_END + 1)).collect()[0]
        assert row["flag_unusual_hour"] is False

    def test_unusual_hour_silent_midday(self, spark):
        row = add_rule_flags(make_txn(spark, hour=14)).collect()[0]
        assert row["flag_unusual_hour"] is False

    # high_velocity rule
    def test_velocity_fires_at_threshold(self, spark):
        row = add_rule_flags(make_txn(spark, velocity=FRAUD_VELOCITY_COUNT_THRESHOLD)).collect()[0]
        assert row["flag_high_velocity"] is True

    def test_velocity_fires_above_threshold(self, spark):
        row = add_rule_flags(make_txn(spark, velocity=FRAUD_VELOCITY_COUNT_THRESHOLD + 3)).collect()[0]
        assert row["flag_high_velocity"] is True

    def test_velocity_silent_below_threshold(self, spark):
        row = add_rule_flags(make_txn(spark, velocity=FRAUD_VELOCITY_COUNT_THRESHOLD - 1)).collect()[0]
        assert row["flag_high_velocity"] is False

    def test_missing_velocity_column_defaults_to_1(self, spark):
        """If the window aggregation hasn't been joined yet, the column won't exist.
        We default it to 1 so the pipeline doesn't crash — velocity rule stays silent.
        """
        df = spark.createDataFrame([Row(
            transaction_id="T-NO-VEL",
            account_id="ACC-TEST",
            amount=50.0,
            timestamp=ts(10),
        )])
        row = add_rule_flags(df).collect()[0]
        assert row["flag_high_velocity"] is False


# ── compute_fraud_score ───────────────────────────────────────────────────────

class TestFraudScore:

    def _score(self, spark, *, large=False, unusual=False, velocity=False):
        """Build a flagged row and compute its score. Returns the Row."""
        df = make_txn(
            spark,
            amount=FRAUD_LARGE_AMOUNT_THRESHOLD + 1 if large else 100.0,
            hour=FRAUD_UNUSUAL_HOUR_START if unusual else 10,
            velocity=FRAUD_VELOCITY_COUNT_THRESHOLD if velocity else 1,
        )
        return compute_fraud_score(add_rule_flags(df)).collect()[0]

    def test_clean_transaction_scores_zero(self, spark):
        row = self._score(spark)
        assert row["fraud_score"] == 0

    def test_clean_transaction_has_empty_reasons(self, spark):
        row = self._score(spark)
        assert row["fraud_reasons"] == ""

    def test_single_rule_scores_33(self, spark):
        assert self._score(spark, large=True)["fraud_score"] == 33

    def test_two_rules_score_66(self, spark):
        assert self._score(spark, large=True, unusual=True)["fraud_score"] == 66

    def test_all_three_rules_score_99(self, spark):
        # 33*3 = 99, which also happens to be the cap — so the cap doesn't change anything here
        assert self._score(spark, large=True, unusual=True, velocity=True)["fraud_score"] == 99

    def test_score_never_exceeds_99(self, spark):
        # Explicitly checking the cap logic. If someone adds a 4th rule later,
        # the cap keeps the score bounded.
        row = self._score(spark, large=True, unusual=True, velocity=True)
        assert row["fraud_score"] <= 99

    def test_reasons_string_contains_triggered_rule(self, spark):
        row = self._score(spark, large=True)
        assert "large_amount" in row["fraud_reasons"]

    def test_reasons_string_comma_separated_multi_rule(self, spark):
        row = self._score(spark, large=True, unusual=True)
        reasons = row["fraud_reasons"].split(",")
        assert "large_amount" in reasons
        assert "unusual_hour" in reasons

    def test_untriggered_rule_absent_from_reasons(self, spark):
        # Only large_amount fires — velocity should not appear in the reasons string
        row = self._score(spark, large=True)
        assert "high_velocity" not in row["fraud_reasons"]


# ── filter_fraud_alerts ───────────────────────────────────────────────────────

class TestFilterAlerts:

    def test_normal_transaction_excluded(self, spark):
        df = make_txn(spark, amount=50.0, hour=10, velocity=1, txn_id="CLEAN")
        df = compute_fraud_score(add_rule_flags(df))
        assert filter_fraud_alerts(df).count() == 0

    def test_flagged_transaction_included(self, spark):
        df = make_txn(spark, amount=FRAUD_LARGE_AMOUNT_THRESHOLD + 1, txn_id="FRAUD")
        df = compute_fraud_score(add_rule_flags(df))
        assert filter_fraud_alerts(df).count() == 1

    def test_mixed_batch_only_returns_flagged(self, spark):
        """This mirrors a real micro-batch: most txns are fine, one is fraud.
        The filter should pass through exactly the right one.
        """
        rows = [
            Row(transaction_id="OK-1",    account_id="ACC-A", amount=120.0,
                timestamp=ts(9),  txn_count_in_window=1),
            Row(transaction_id="OK-2",    account_id="ACC-B", amount=500.0,
                timestamp=ts(15), txn_count_in_window=2),
            Row(transaction_id="FRAUD-1", account_id="ACC-C",
                amount=float(FRAUD_LARGE_AMOUNT_THRESHOLD + 5000),
                timestamp=ts(2),  txn_count_in_window=1),   # large amount AND unusual hour
        ]
        df = spark.createDataFrame(rows)
        df = compute_fraud_score(add_rule_flags(df))
        alerts = filter_fraud_alerts(df)

        ids = {r["transaction_id"] for r in alerts.collect()}
        assert ids == {"FRAUD-1"}

    def test_all_clean_returns_empty(self, spark):
        rows = [
            Row(transaction_id=f"OK-{i}", account_id="ACC-X",
                amount=float(i * 10), timestamp=ts(10), txn_count_in_window=1)
            for i in range(1, 6)
        ]
        df = compute_fraud_score(add_rule_flags(spark.createDataFrame(rows)))
        assert filter_fraud_alerts(df).count() == 0
