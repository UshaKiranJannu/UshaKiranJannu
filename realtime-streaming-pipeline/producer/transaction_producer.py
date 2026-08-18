"""
transaction_producer.py

Simulates a stream of financial transactions and publishes them to Kafka.

I inject a configurable percentage of anomalous transactions (default 10%) so
the downstream Spark job has something real to detect. Without this, every
transaction looks normal and the fraud detector never does anything interesting.

Three types of anomalies are generated randomly:
  - large_amount: amount > $10k, normal hour
  - unusual_hour: normal amount, transaction at midnight–4 AM
  - both: large amount AND unusual hour (highest fraud score)

Usage:
    python producer/transaction_producer.py
"""

import json
import time
import uuid
import random
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer
from loguru import logger

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.pipeline_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_TOPIC,
    PRODUCER_SLEEP_SECONDS,
    PRODUCER_BATCH_SIZE,
    ANOMALY_INJECTION_RATE,
    TRANSACTION_TYPES,
    MERCHANT_CATEGORIES,
    LOCATIONS,
    FRAUD_LARGE_AMOUNT_THRESHOLD,
    FRAUD_UNUSUAL_HOUR_START,
    FRAUD_UNUSUAL_HOUR_END,
)

fake = Faker()


def make_producer() -> KafkaProducer:
    """Create a KafkaProducer with JSON serialisation and basic retry config.

    acks="all" means the broker waits for all in-sync replicas before acking —
    slightly slower but avoids message loss if a broker goes down mid-write.
    For a demo this is overkill, but it's the right default to build the habit.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_block_ms=10_000,
    )


def generate_normal_transaction(account_id: str) -> dict:
    """Generate a realistic but non-anomalous transaction."""
    amount = round(random.uniform(5.0, 2_000.0), 2)
    hour   = random.randint(FRAUD_UNUSUAL_HOUR_END + 1, 23)  # business hours

    ts = int(datetime.now().timestamp() * 1000)

    return {
        "transaction_id":    str(uuid.uuid4()),
        "account_id":        account_id,
        "customer_name":     fake.name(),
        "transaction_type":  random.choice(TRANSACTION_TYPES),
        "amount":            amount,
        "currency":          "USD",
        "merchant":          fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "location":          random.choice(LOCATIONS),
        "timestamp":         ts,
        "is_anomalous":      False,
    }


def generate_anomalous_transaction(account_id: str) -> dict:
    """Generate a transaction with one or more fraud signals deliberately injected.

    The three anomaly types map to the three rules in fraud_detector.py — if you
    add a new rule there, add a matching anomaly type here so the producer
    generates data that exercises it.
    """
    anomaly_type = random.choice(["large_amount", "unusual_hour", "both"])

    if anomaly_type == "large_amount":
        amount = round(random.uniform(FRAUD_LARGE_AMOUNT_THRESHOLD + 1, 50_000.0), 2)
        hour   = random.randint(8, 20)
    elif anomaly_type == "unusual_hour":
        amount = round(random.uniform(5.0, 2_000.0), 2)
        hour   = random.randint(FRAUD_UNUSUAL_HOUR_START, FRAUD_UNUSUAL_HOUR_END)
    else:  # both
        amount = round(random.uniform(FRAUD_LARGE_AMOUNT_THRESHOLD + 1, 50_000.0), 2)
        hour   = random.randint(FRAUD_UNUSUAL_HOUR_START, FRAUD_UNUSUAL_HOUR_END)

    # Build a timestamp at the chosen hour (today)
    now = datetime.now()
    ts_dt = now.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))
    ts = int(ts_dt.timestamp() * 1000)

    return {
        "transaction_id":    str(uuid.uuid4()),
        "account_id":        account_id,
        "customer_name":     fake.name(),
        "transaction_type":  random.choice(TRANSACTION_TYPES),
        "amount":            amount,
        "currency":          "USD",
        "merchant":          fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "location":          random.choice(LOCATIONS),
        "timestamp":         ts,
        "is_anomalous":      True,
    }


def generate_transaction(account_id: str) -> dict:
    """Randomly pick normal vs anomalous based on ANOMALY_INJECTION_RATE."""
    if random.random() < ANOMALY_INJECTION_RATE:
        return generate_anomalous_transaction(account_id)
    return generate_normal_transaction(account_id)


def run_producer(num_accounts: int = 20) -> None:
    """
    Continuously produce transactions to Kafka.

    Parameters
    ----------
    num_accounts : int
        Number of simulated account IDs to spread transactions across.
    """
    producer = make_producer()
    account_pool = [f"ACC-{i:05d}" for i in range(1, num_accounts + 1)]

    logger.info(
        "Producer started → topic='{}', bootstrap='{}'",
        KAFKA_RAW_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    total_sent = 0
    try:
        while True:
            batch = []
            for _ in range(PRODUCER_BATCH_SIZE):
                account_id  = random.choice(account_pool)
                transaction = generate_transaction(account_id)
                batch.append(transaction)

                producer.send(
                    KAFKA_RAW_TOPIC,
                    key=transaction["account_id"],
                    value=transaction,
                )

            producer.flush()
            total_sent += len(batch)
            anomalous   = sum(1 for t in batch if t["is_anomalous"])
            logger.info(
                "Batch sent | total={} | batch_size={} | anomalous_in_batch={}",
                total_sent, len(batch), anomalous,
            )
            time.sleep(PRODUCER_SLEEP_SECONDS)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user. Total messages sent: {}", total_sent)
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    run_producer()
