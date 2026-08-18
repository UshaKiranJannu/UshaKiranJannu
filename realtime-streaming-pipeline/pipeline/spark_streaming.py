"""
spark_streaming.py

Entry point for the Spark Structured Streaming job.

Data flow:
  Kafka (raw-transactions)
    → parse JSON
    → watermarked 5-min sliding window aggregation per account (for velocity)
    → fraud_detector: flag rules, compute score
    → write everything to Snowflake RAW_TRANSACTIONS  (or ./output/ locally)
    → write flagged records to Snowflake FLAGGED_TRANSACTIONS + Kafka fraud-alerts

Sink selection — Snowflake vs local Parquet:
  If SNOWFLAKE_ACCOUNT is set to a real value in .env, the job writes to Snowflake.
  Otherwise it falls back to writing Parquet files under ./output/ so the full
  pipeline runs locally without any cloud account. Same fraud detection logic,
  same Kafka alerts — just a different storage destination.

  To force local mode regardless:  set SINK_MODE=local in .env
  To force Snowflake:               set SINK_MODE=snowflake in .env
  Default: auto-detect from SNOWFLAKE_ACCOUNT

A few other decisions baked in here:
  - foreachBatch instead of native sinks — gives a normal batch DataFrame so
    you can write to multiple destinations and apply any transformation.
  - Checkpoints under ./checkpoints/ so the job recovers from failures without
    reprocessing the whole topic.
  - Window aggregation runs as a separate query; velocity count defaults to 1
    when the window data isn't available yet (fraud_detector handles that).

Run locally (no Snowflake needed):
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        pipeline/spark_streaming.py
"""

from __future__ import annotations

import os
import sys

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.pipeline_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_TOPIC,
    KAFKA_ALERTS_TOPIC,
    KAFKA_GROUP_ID,
    SPARK_APP_NAME,
    SPARK_MASTER,
    CHECKPOINT_LOCATION,
    WINDOW_DURATION,
    SLIDE_DURATION,
    WATERMARK_DELAY,
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
from pipeline.schemas import TRANSACTION_SCHEMA
from pipeline.fraud_detector import add_rule_flags, compute_fraud_score, filter_fraud_alerts

# ── Sink mode detection ───────────────────────────────────────────────────────
# Placeholder values in .env mean Snowflake isn't configured yet — fall back
# to local Parquet so the pipeline runs without a cloud account.
_PLACEHOLDER_ACCOUNTS = {"", "your_account", "dummy"}

def _snowflake_configured() -> bool:
    explicit = os.getenv("SINK_MODE", "").lower()
    if explicit == "snowflake":
        return True
    if explicit == "local":
        return False
    return SNOWFLAKE_ACCOUNT not in _PLACEHOLDER_ACCOUNTS

USE_SNOWFLAKE = _snowflake_configured()
LOCAL_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")

if USE_SNOWFLAKE:
    logger.info("Sink mode: SNOWFLAKE  (account={})", SNOWFLAKE_ACCOUNT)
else:
    logger.info("Sink mode: LOCAL PARQUET  (dir={})", LOCAL_OUTPUT_DIR)
    logger.info("Set SNOWFLAKE_ACCOUNT in .env (or SINK_MODE=snowflake) to write to Snowflake instead.")


# ── Snowflake helpers ─────────────────────────────────────────────────────────

def snowflake_options(table: str) -> dict:
    return {
        "sfURL":       f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
        "sfUser":      SNOWFLAKE_USER,
        "sfPassword":  SNOWFLAKE_PASSWORD,
        "sfDatabase":  SNOWFLAKE_DATABASE,
        "sfSchema":    SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "sfRole":      SNOWFLAKE_ROLE,
        "dbtable":     table,
    }


# ── Unified write function ────────────────────────────────────────────────────

def write_batch(df: DataFrame, table_name: str, partition_col: str | None = None) -> None:
    """Write a micro-batch DataFrame to either Snowflake or local Parquet.

    table_name is used as the Snowflake table name AND as the subdirectory name
    under ./output/ — so the same call works for both sinks.
    """
    if df.isEmpty():
        return

    if USE_SNOWFLAKE:
        (
            df.write
            .format("net.snowflake.spark.snowflake")
            .options(**snowflake_options(table_name))
            .mode("append")
            .save()
        )
    else:
        path = os.path.join(LOCAL_OUTPUT_DIR, table_name.lower())
        writer = df.write.mode("append")
        if partition_col and partition_col in df.columns:
            writer = writer.partitionBy(partition_col)
        writer.parquet(path)
        logger.debug("Wrote batch to {}", path)


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    if USE_SNOWFLAKE:
        packages += ",net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4"

    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ── Read from Kafka ───────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read and parse the raw-transactions topic into a typed DataFrame."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", KAFKA_GROUP_ID)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
    ).select("data.*")

    # event_time is what Spark uses for watermarking — it has to come from the
    # data itself (not processing time) so late arrivals land in the right window
    return parsed.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp") / 1000),
    )


# ── Windowed aggregation ──────────────────────────────────────────────────────

def build_window_aggregation(df: DataFrame) -> DataFrame:
    """5-min sliding window per account — used to detect velocity-based fraud."""
    return (
        df.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.col("account_id"),
            F.window(F.col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
        )
        .agg(
            F.count("*").alias("txn_count_in_window"),
            F.sum("amount").alias("total_amount"),
            F.max("amount").alias("max_amount"),
            F.approx_count_distinct("location").alias("unique_locations"),
            F.approx_count_distinct("merchant").alias("unique_merchants"),
        )
        .select(
            F.col("account_id"),
            F.col("window.start").cast("long").alias("window_start"),
            F.col("window.end").cast("long").alias("window_end"),
            F.col("txn_count_in_window"),
            F.col("total_amount"),
            F.col("max_amount"),
            F.col("unique_locations"),
            F.col("unique_merchants"),
        )
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = read_kafka_stream(spark)

    # Window aggregation query — runs continuously, writes velocity stats
    window_query = (
        build_window_aggregation(raw_stream)
        .writeStream
        .foreachBatch(lambda df, _: write_batch(df, "WINDOW_AGGREGATIONS"))
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/window_agg")
        .outputMode("update")
        .start()
    )

    # Main fraud detection query
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        enriched = add_rule_flags(batch_df)
        enriched = compute_fraud_score(enriched)
        enriched = enriched.withColumn("alert_timestamp", F.unix_timestamp() * 1000)

        # All enriched transactions → RAW_TRANSACTIONS (or output/raw_transactions/)
        write_batch(enriched, SNOWFLAKE_RAW_TABLE, partition_col="transaction_type")

        alerts = filter_fraud_alerts(enriched)
        if alerts.isEmpty():
            return

        # Flagged transactions → FLAGGED_TRANSACTIONS (or output/flagged_transactions/)
        write_batch(alerts, SNOWFLAKE_FLAGGED_TABLE)

        # Also publish fraud alerts back to Kafka so other consumers can react
        alert_cols = [c for c in [
            "transaction_id", "account_id", "amount", "transaction_type",
            "location", "timestamp", "fraud_score", "fraud_reasons", "alert_timestamp",
        ] if c in alerts.columns]

        (
            alerts
            .select(
                F.col("account_id").alias("key"),
                F.to_json(F.struct(*alert_cols)).alias("value"),
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", KAFKA_ALERTS_TOPIC)
            .save()
        )

        flagged_count = alerts.count()
        logger.info(
            "Batch {} | flagged={} | sink={}",
            batch_id, flagged_count, "snowflake" if USE_SNOWFLAKE else "local"
        )

    main_query = (
        raw_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/main")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
