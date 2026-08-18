import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ─── Kafka Configuration ───────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC         = "raw-transactions"
KAFKA_ALERTS_TOPIC      = "fraud-alerts"
KAFKA_METRICS_TOPIC     = "pipeline-metrics"
KAFKA_GROUP_ID          = "fraud-detection-group"

# ─── Spark Configuration ───────────────────────────────────────────────────────
SPARK_APP_NAME          = "FinancialFraudDetectionPipeline"
SPARK_MASTER            = "local[*]"
CHECKPOINT_LOCATION     = "./checkpoints/streaming"

# ─── Streaming Window Configuration ───────────────────────────────────────────
WINDOW_DURATION         = "5 minutes"   # Aggregation window size
SLIDE_DURATION          = "1 minute"    # How often window slides
WATERMARK_DELAY         = "2 minutes"   # Max lateness of arriving events

# ─── Fraud Detection Thresholds ────────────────────────────────────────────────
FRAUD_LARGE_AMOUNT_THRESHOLD    = 10000.0   # Flag transactions above $10,000
FRAUD_VELOCITY_COUNT_THRESHOLD  = 5         # Max transactions per account per window
FRAUD_UNUSUAL_HOUR_START        = 0         # Midnight
FRAUD_UNUSUAL_HOUR_END          = 4         # 4 AM

# ─── Snowflake Configuration ───────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "FINANCE_DB")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "TRANSACTIONS")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

# Snowflake table names
SNOWFLAKE_RAW_TABLE     = "RAW_TRANSACTIONS"
SNOWFLAKE_FLAGGED_TABLE = "FLAGGED_TRANSACTIONS"

# ─── Producer Configuration ────────────────────────────────────────────────────
PRODUCER_SLEEP_SECONDS      = 0.5    # Time between each transaction event
PRODUCER_BATCH_SIZE         = 10     # Transactions per batch
ANOMALY_INJECTION_RATE      = 0.1    # 10% of transactions will be anomalous

# ─── Transaction Types ─────────────────────────────────────────────────────────
TRANSACTION_TYPES = ["payment", "transfer", "withdrawal", "deposit", "refund"]

# ─── Merchant Categories ───────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "grocery", "electronics", "restaurant", "travel",
    "healthcare", "entertainment", "retail", "fuel"
]

# ─── US Cities for Location Simulation ────────────────────────────────────────
LOCATIONS = [
    "New York", "Los Angeles", "Chicago", "Houston", "Charlotte",
    "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"
]
