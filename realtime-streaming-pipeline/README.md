# Real-Time Fraud Detection Pipeline

![CI](https://github.com/ushakiran-jannu/realtime-streaming-pipeline/actions/workflows/ci.yml/badge.svg)

A streaming data engineering project that detects suspicious financial transactions in real time using Kafka, Spark Structured Streaming, Snowflake, and dbt.

I built this to demonstrate the full end-to-end pattern for a streaming analytics pipeline — not just the Spark job in isolation, but everything around it: the producer that generates realistic data, the fraud detection logic with proper watermarking, the Snowflake schema that lands the results, and the dbt models that turn raw tables into something a BI tool can actually use.

---

## What it does

A Kafka producer continuously generates simulated bank transactions — payments, transfers, withdrawals — spread across 20 accounts. About 10% of those transactions have fraud signals injected (large amounts, unusual hours, or both).

A Spark Structured Streaming job consumes that topic, applies a sliding 5-minute window aggregation to track transaction velocity per account, scores each transaction against three fraud rules, and writes results to two destinations:
- **Snowflake** — all enriched transactions land in `RAW_TRANSACTIONS`, flagged ones go to `FLAGGED_TRANSACTIONS`
- **Kafka** — fraud alerts are published back to a `fraud-alerts` topic so other consumers can react in real time

dbt models then transform those raw tables into analytics-ready marts that a dashboard can query directly.

---

## Architecture

```
┌─────────────────┐     raw-transactions      ┌──────────────────────────┐
│  Transaction    │ ─────────────────────────▶ │  Spark Structured        │
│  Producer       │                            │  Streaming               │
│  (Faker + Kafka)│                            │                          │
└─────────────────┘                            │  - JSON parse            │
                                               │  - 5-min window agg      │
                                               │  - Fraud scoring         │
                                               │  - Watermark (2 min)     │
                                               └──────────┬───────────────┘
                                                          │
                              ┌───────────────────────────┼──────────────────┐
                              ▼                           ▼                  ▼
                    ┌─────────────────┐       ┌──────────────────┐   ┌──────────────┐
                    │   Snowflake     │       │   Snowflake      │   │  Kafka       │
                    │ RAW_TRANSACTIONS│       │ FLAGGED_TXNS     │   │ fraud-alerts │
                    └────────┬────────┘       └──────────────────┘   └──────────────┘
                             │
                    ┌────────▼────────┐
                    │   dbt models    │
                    │  staging →      │
                    │  intermediate → │
                    │  marts          │
                    └─────────────────┘
```

---

## Fraud detection rules

Three heuristic rules, each contributing 33 points to a `fraud_score` (max 99):

| Rule | Condition | Points |
|---|---|---|
| `large_amount` | transaction > $10,000 | 33 |
| `unusual_hour` | transaction between midnight and 4 AM | 33 |
| `high_velocity` | same account, 5+ transactions in the current 5-min window | 33 |

A transaction that triggers all three gets a score of 99. The score and the comma-separated list of triggered rules (`fraud_reasons`) are written to Snowflake and the Kafka alerts topic.

The rule logic is isolated in `pipeline/fraud_detector.py` and is fully unit-tested — the Spark job itself is kept thin so it's easy to swap in a model-based approach later.

---

## Project structure

```
realtime-streaming-pipeline/
├── producer/
│   └── transaction_producer.py   # Kafka producer with anomaly injection
├── pipeline/
│   ├── schemas.py                 # PySpark schemas for Kafka topics
│   ├── fraud_detector.py          # Rule-based fraud scoring (pure Spark functions)
│   ├── spark_streaming.py         # Streaming job entry point
│   └── snowflake_writer.py        # DDL + schema init helper
├── airflow/dags/
│   └── fraud_pipeline_dag.py      # Orchestration: health check → Snowflake init → dbt
├── dbt/
│   ├── models/staging/            # stg_raw_transactions, stg_flagged_transactions
│   ├── models/intermediate/       # account risk summary, hourly volume
│   └── models/marts/              # fraud_account_risk_mart, transaction_volume_mart
├── tests/
│   ├── conftest.py                # shared SparkSession fixture
│   ├── test_fraud_detector.py     # unit tests for all fraud rules + scoring
│   └── test_producer.py           # unit tests for transaction generation + anomaly rate
├── config/pipeline_config.py      # all thresholds and connection params
├── docker-compose.yml             # Zookeeper, Kafka, Kafka UI, producer, Spark
├── Dockerfile
└── Makefile
```

---

## Running it locally

**Prerequisites:** Docker, Python 3.11+, Java 17 (for Spark)

**1. Start Kafka**
```bash
make up
# Kafka UI available at http://localhost:8080
```

**2. Initialize Snowflake tables** (once, requires real Snowflake credentials in `.env`)
```bash
make init-sf
```

**3. Start producing transactions**
```bash
make producer
# Or inside Docker: make up-all
```

**4. Start the Spark streaming job**
```bash
make spark
# Expects spark-submit on your PATH with the Kafka package available
```

**5. Run dbt transformations** (after data starts flowing into Snowflake)
```bash
make dbt-run
make dbt-test
```

---

## Running the tests

Tests use a local SparkSession — no Kafka or Snowflake needed.

```bash
make test
# or: pytest tests/ -v
```

The test suite covers:
- Each fraud rule in isolation (positive and negative cases)
- Edge cases: exact threshold boundary, missing columns, empty batches
- Transaction generation: schema correctness, anomaly rate within expected tolerance
- Mixed batches: only the right transactions get flagged

---

## Design decisions worth noting

**Why not manage the Spark job in Airflow?**
Spark Structured Streaming jobs are long-running processes, not tasks — they don't start, run for a minute, and finish. Forcing them into an Airflow task creates operational complexity (what does "retry" mean for a streaming job?). The Airflow DAG here handles orchestration of the surrounding system — health checks, schema init, dbt refreshes — and the Spark job runs independently via spark-submit or container orchestration.

**Why rule-based instead of ML?**
Explainability and testability. Rules are easy to verify (I can write a unit test that checks exactly when each rule fires), easy to tune (change a threshold in `pipeline_config.py`), and easy to explain to a non-technical stakeholder. In production you'd want a model on top, but the infrastructure pattern is identical — just replace `fraud_detector.py` with a UDF that calls your model endpoint.

**Why watermarking at 2 minutes?**
The 5-minute window with a 2-minute watermark means Spark will wait up to 2 minutes for late-arriving events before finalizing a window. In practice this trades a bit of output latency for correctness — you don't want a velocity alert to miss a transaction just because it arrived 90 seconds late due to a producer hiccup.

---

## Stack

- **Apache Kafka** (Confluent 7.5) — event streaming
- **Apache Spark 3.5** (PySpark) — structured streaming + batch processing
- **Snowflake** — data warehouse target
- **dbt-snowflake 1.9** — analytics engineering / transformation layer
- **Apache Airflow 2.9** — orchestration
- **Docker Compose** — local development environment
- **pytest** — unit testing

---

*Usha Kiran Jannu — [LinkedIn](https://www.linkedin.com/in/usha-kiran-jannu/) · [jannu.usha.kiran12@gmail.com](mailto:jannu.usha.kiran12@gmail.com)*
