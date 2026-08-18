"""
fraud_pipeline_dag.py
----------------------
Airflow DAG that orchestrates the full real-time fraud-detection pipeline:

  1. health_check_kafka      – verify Kafka broker is reachable
  2. init_snowflake_schema   – idempotently create Snowflake tables
  3. start_transaction_producer – launch the Kafka producer (background)
  4. run_dbt_staging         – dbt run for staging models
  5. run_dbt_intermediate    – dbt run for intermediate models
  6. run_dbt_marts           – dbt run for mart models
  7. run_dbt_tests           – dbt test suite

The Spark streaming job is long-running and is NOT managed by Airflow here;
it should be submitted separately (e.g. via a BashOperator in a separate
monitoring DAG or via spark-submit in your container orchestration layer).

Schedule: every hour (@hourly) — the dbt models refresh Snowflake views
built on top of the tables the Spark job populates continuously.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import sys
import os

# Make sure our project modules are importable inside Airflow workers
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)


# ─── Default args ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
}

DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt")


# ─── Python callables ─────────────────────────────────────────────────────────

def health_check_kafka(**kwargs) -> None:
    """Ping Kafka to confirm the broker is up before starting anything."""
    from kafka import KafkaAdminClient
    from config.pipeline_config import KAFKA_BOOTSTRAP_SERVERS

    client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=5000)
    topics = client.list_topics()
    client.close()
    print(f"Kafka healthy. Known topics: {topics}")


def init_snowflake(**kwargs) -> None:
    """Create Snowflake tables if they don't exist yet."""
    from pipeline.snowflake_writer import init_schema
    init_schema()


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="fraud_detection_pipeline",
    description="Orchestrates Kafka health-check, Snowflake init, and dbt transformations",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "fraud", "kafka", "snowflake", "dbt"],
) as dag:

    # ── Task 1: Kafka health check ────────────────────────────────────────────
    t_kafka_health = PythonOperator(
        task_id="health_check_kafka",
        python_callable=health_check_kafka,
        doc_md="Verify the Kafka broker at KAFKA_BOOTSTRAP_SERVERS is reachable.",
    )

    # ── Task 2: Snowflake schema init ─────────────────────────────────────────
    t_init_snowflake = PythonOperator(
        task_id="init_snowflake_schema",
        python_callable=init_snowflake,
        doc_md="Idempotently create all target Snowflake tables.",
    )

    # ── Task 3: Start producer (fire-and-forget; runs in background) ──────────
    t_start_producer = BashOperator(
        task_id="start_transaction_producer",
        bash_command=(
            f"nohup python {PROJECT_ROOT}/producer/transaction_producer.py "
            f"> /tmp/producer.log 2>&1 & echo 'Producer PID:' $!"
        ),
        doc_md="Launch the Kafka transaction producer in the background.",
    )

    # ── Task 4: dbt staging models ────────────────────────────────────────────
    t_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select staging --profiles-dir . --no-use-colors"
        ),
        doc_md="Run all staging dbt models.",
    )

    # ── Task 5: dbt intermediate models ──────────────────────────────────────
    t_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select intermediate --profiles-dir . --no-use-colors"
        ),
        doc_md="Run all intermediate dbt models.",
    )

    # ── Task 6: dbt mart models ───────────────────────────────────────────────
    t_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select marts --profiles-dir . --no-use-colors"
        ),
        doc_md="Run all mart dbt models.",
    )

    # ── Task 7: dbt tests ─────────────────────────────────────────────────────
    t_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir . --no-use-colors"
        ),
        doc_md="Run the full dbt test suite.",
    )

    # ─── Task dependencies ────────────────────────────────────────────────────
    (
        t_kafka_health
        >> t_init_snowflake
        >> t_start_producer
        >> t_dbt_staging
        >> t_dbt_intermediate
        >> t_dbt_marts
        >> t_dbt_tests
    )
