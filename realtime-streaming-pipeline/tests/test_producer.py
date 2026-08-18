"""
test_producer.py

Unit tests for the transaction producer logic. No Kafka broker needed here —
I'm only testing the data generation functions, which are pure Python.

The key things I want to verify:
  1. Schema — every generated record has all the fields the Spark consumer expects
  2. Normal transactions are actually normal (amount range, business hours, not flagged)
  3. Anomalous transactions always have at least one detectable fraud signal
  4. The overall anomaly injection rate stays near the configured 10% target
"""

import uuid
from datetime import datetime

import pytest

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from producer.transaction_producer import (
    generate_normal_transaction,
    generate_anomalous_transaction,
    generate_transaction,
)
from config.pipeline_config import (
    TRANSACTION_TYPES,
    MERCHANT_CATEGORIES,
    LOCATIONS,
    FRAUD_LARGE_AMOUNT_THRESHOLD,
    FRAUD_UNUSUAL_HOUR_START,
    FRAUD_UNUSUAL_HOUR_END,
    ANOMALY_INJECTION_RATE,
)

ACC = "ACC-00001"

# Fields Spark's TRANSACTION_SCHEMA expects — if the producer drops one of these
# the stream will silently produce nulls, which is a pain to debug.
REQUIRED_FIELDS = [
    "transaction_id", "account_id", "customer_name",
    "transaction_type", "amount", "currency",
    "merchant", "merchant_category", "location",
    "timestamp", "is_anomalous",
]


# ── normal transactions ───────────────────────────────────────────────────────

class TestNormalTransaction:

    def test_all_required_fields_present(self):
        txn = generate_normal_transaction(ACC)
        missing = [f for f in REQUIRED_FIELDS if f not in txn]
        assert not missing, f"Missing fields: {missing}"

    def test_transaction_id_is_valid_uuid(self):
        txn = generate_normal_transaction(ACC)
        # Should not raise
        parsed = uuid.UUID(txn["transaction_id"])
        assert str(parsed) == txn["transaction_id"]

    def test_account_id_passes_through(self):
        txn = generate_normal_transaction(ACC)
        assert txn["account_id"] == ACC

    def test_is_anomalous_always_false(self):
        # Running 30 times because this is a random function — want to rule out
        # an off-by-one where someone accidentally flipped the flag
        for _ in range(30):
            assert generate_normal_transaction(ACC)["is_anomalous"] is False

    def test_amount_within_expected_range(self):
        for _ in range(50):
            amount = generate_normal_transaction(ACC)["amount"]
            assert 5.0 <= amount <= 2000.0, f"Amount {amount} outside normal range"

    def test_amount_below_fraud_threshold(self):
        # Normal transactions should never accidentally cross the fraud threshold
        for _ in range(50):
            amount = generate_normal_transaction(ACC)["amount"]
            assert amount <= FRAUD_LARGE_AMOUNT_THRESHOLD

    def test_currency_is_usd(self):
        assert generate_normal_transaction(ACC)["currency"] == "USD"

    def test_transaction_type_is_known(self):
        for _ in range(20):
            assert generate_normal_transaction(ACC)["transaction_type"] in TRANSACTION_TYPES

    def test_location_is_known_city(self):
        for _ in range(20):
            assert generate_normal_transaction(ACC)["location"] in LOCATIONS

    def test_merchant_category_is_known(self):
        for _ in range(20):
            assert generate_normal_transaction(ACC)["merchant_category"] in MERCHANT_CATEGORIES

    def test_timestamp_is_positive_epoch_ms(self):
        txn = generate_normal_transaction(ACC)
        assert isinstance(txn["timestamp"], int)
        # Epoch-ms for any date after 2020 is > 1.5 trillion
        assert txn["timestamp"] > 1_500_000_000_000

    def test_merchant_name_is_non_empty_string(self):
        txn = generate_normal_transaction(ACC)
        assert isinstance(txn["merchant"], str)
        assert len(txn["merchant"]) > 0

    def test_customer_name_is_non_empty_string(self):
        txn = generate_normal_transaction(ACC)
        assert isinstance(txn["customer_name"], str)
        assert len(txn["customer_name"]) > 0


# ── anomalous transactions ────────────────────────────────────────────────────

class TestAnomalousTransaction:

    def test_is_anomalous_always_true(self):
        for _ in range(30):
            assert generate_anomalous_transaction(ACC)["is_anomalous"] is True

    def test_always_has_detectable_fraud_signal(self):
        """Every anomalous transaction must have at least one signal the detector will catch.
        Testing 100 samples covers all three anomaly_type branches statistically.
        """
        for _ in range(100):
            txn = generate_anomalous_transaction(ACC)
            hour = datetime.fromtimestamp(txn["timestamp"] / 1000).hour
            is_large = txn["amount"] > FRAUD_LARGE_AMOUNT_THRESHOLD
            is_odd_hour = FRAUD_UNUSUAL_HOUR_START <= hour <= FRAUD_UNUSUAL_HOUR_END
            assert is_large or is_odd_hour, (
                f"Anomalous txn has no detectable signal: amount={txn['amount']}, hour={hour}"
            )

    def test_schema_identical_to_normal(self):
        """Anomalous and normal transactions must have the same schema — the Kafka
        consumer uses a single schema for the whole topic."""
        normal = set(generate_normal_transaction(ACC).keys())
        anomalous = set(generate_anomalous_transaction(ACC).keys())
        assert normal == anomalous

    def test_large_amount_variant_exceeds_threshold(self):
        """When anomaly_type='large_amount', the amount must actually cross the threshold.
        Pin the RNG to force this branch."""
        import random
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(random, "choice", lambda _: "large_amount")
            txn = generate_anomalous_transaction(ACC)
            assert txn["amount"] > FRAUD_LARGE_AMOUNT_THRESHOLD

    def test_unusual_hour_variant_falls_in_window(self):
        """When anomaly_type='unusual_hour', the transaction hour must be in [0, 4]."""
        import random
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(random, "choice", lambda _: "unusual_hour")
            txn = generate_anomalous_transaction(ACC)
            hour = datetime.fromtimestamp(txn["timestamp"] / 1000).hour
            assert FRAUD_UNUSUAL_HOUR_START <= hour <= FRAUD_UNUSUAL_HOUR_END


# ── injection rate ────────────────────────────────────────────────────────────

class TestInjectionRate:

    def test_anomaly_rate_near_configured_value(self):
        """Over 2000 samples the anomaly rate should land within ±4% of the target.
        Using 2000 instead of 1000 to reduce flakiness — this is a property test
        and we want it green on CI without a fixed seed.
        """
        n = 2000
        hits = sum(1 for _ in range(n) if generate_transaction(ACC)["is_anomalous"])
        actual_rate = hits / n
        tolerance = 0.04
        assert abs(actual_rate - ANOMALY_INJECTION_RATE) <= tolerance, (
            f"Anomaly rate {actual_rate:.2%} is more than {tolerance:.0%} away "
            f"from the configured {ANOMALY_INJECTION_RATE:.2%}"
        )

    def test_generate_transaction_returns_valid_schema(self):
        txn = generate_transaction(ACC)
        missing = [f for f in REQUIRED_FIELDS if f not in txn]
        assert not missing, f"generate_transaction missing fields: {missing}"
