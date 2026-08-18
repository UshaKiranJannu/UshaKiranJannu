# ─── Makefile ─────────────────────────────────────────────────────────────────
# Convenience targets for the realtime-streaming-pipeline project.
#
# Usage:
#   make up          – start Kafka + UI
#   make up-all      – start everything (Kafka + producer + Spark)
#   make down        – stop all containers
#   make producer    – run the producer locally (no Docker)
#   make spark       – run the Spark streaming job locally (no Docker)
#   make init-sf     – initialise Snowflake schema
#   make dbt-run     – run all dbt models
#   make dbt-test    – run dbt tests
#   make test        – run the Python test suite
#   make lint        – run flake8

.PHONY: up up-all down producer spark run-local init-sf dbt-run dbt-test test lint

# ── Docker ────────────────────────────────────────────────────────────────────
up:
	docker compose up -d zookeeper kafka kafka-ui

up-all:
	docker compose up -d

down:
	docker compose down -v

# ── Local runs ────────────────────────────────────────────────────────────────
producer:
	python producer/transaction_producer.py

spark:
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		pipeline/spark_streaming.py

# Run with local Parquet output — no Snowflake account needed
run-local:
	SINK_MODE=local spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		pipeline/spark_streaming.py

# ── Snowflake ─────────────────────────────────────────────────────────────────
init-sf:
	python pipeline/snowflake_writer.py --init-schema

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ── Tests & linting ───────────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

lint:
	flake8 pipeline/ producer/ config/ tests/ --max-line-length=100
