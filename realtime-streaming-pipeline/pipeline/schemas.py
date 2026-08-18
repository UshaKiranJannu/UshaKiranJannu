from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, BooleanType, IntegerType
)

# ─── Raw Transaction Schema ────────────────────────────────────────────────────
# This is the schema of every message coming from Kafka topic: raw-transactions
# Each field maps directly to what the producer sends

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),  nullable=False),  # Unique UUID per transaction
    StructField("account_id",       StringType(),  nullable=False),  # Bank account identifier
    StructField("customer_name",    StringType(),  nullable=True),   # Account holder name
    StructField("transaction_type", StringType(),  nullable=False),  # payment/transfer/withdrawal/deposit/refund
    StructField("amount",           DoubleType(),  nullable=False),  # Transaction amount in USD
    StructField("currency",         StringType(),  nullable=True),   # Always USD for now
    StructField("merchant",         StringType(),  nullable=True),   # Merchant name
    StructField("merchant_category",StringType(),  nullable=True),   # grocery/electronics/etc
    StructField("location",         StringType(),  nullable=True),   # City where transaction happened
    StructField("timestamp",        LongType(),    nullable=False),  # Unix epoch milliseconds
    StructField("is_anomalous",     BooleanType(), nullable=True),   # Injected by producer for testing
])

# ─── Fraud Alert Schema ────────────────────────────────────────────────────────
# Schema for messages written to Kafka topic: fraud-alerts
# Extends the transaction with fraud scoring fields

FRAUD_ALERT_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),  nullable=False),
    StructField("account_id",       StringType(),  nullable=False),
    StructField("amount",           DoubleType(),  nullable=False),
    StructField("transaction_type", StringType(),  nullable=False),
    StructField("location",         StringType(),  nullable=True),
    StructField("timestamp",        LongType(),    nullable=False),
    StructField("fraud_score",      IntegerType(), nullable=False),  # 0-100 risk score
    StructField("fraud_reasons",    StringType(),  nullable=True),   # Comma-separated rule names triggered
    StructField("alert_timestamp",  LongType(),    nullable=False),  # When the alert was generated
])

# ─── Window Aggregation Schema ─────────────────────────────────────────────────
# Schema for the 5-minute windowed aggregation output
# Used to detect velocity-based fraud (too many transactions in short time)

WINDOW_AGG_SCHEMA = StructType([
    StructField("account_id",           StringType(),  nullable=False),
    StructField("window_start",         LongType(),    nullable=False),  # Window start epoch ms
    StructField("window_end",           LongType(),    nullable=False),  # Window end epoch ms
    StructField("transaction_count",    IntegerType(), nullable=False),  # Total txns in window
    StructField("total_amount",         DoubleType(),  nullable=False),  # Total spend in window
    StructField("max_amount",           DoubleType(),  nullable=False),  # Largest single txn
    StructField("unique_locations",     LongType(),    nullable=False),  # approx distinct locations
    StructField("unique_merchants",     LongType(),    nullable=False),  # approx distinct merchants
])
