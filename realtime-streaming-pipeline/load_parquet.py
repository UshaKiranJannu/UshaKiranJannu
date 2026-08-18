"""
load_parquet.py
----------------
Loads the Spark output Parquet files into a local DuckDB database
so dbt can run against them without needing Snowflake.

Run from the project root:
    python load_parquet.py

Creates: fraud_analytics.duckdb
Tables:  main.raw_transactions
         main.flagged_transactions
"""

import os
import glob
import duckdb
from loguru import logger

DB_PATH = "fraud_analytics.duckdb"
OUTPUT_DIR = "output"


def load_table(con: duckdb.DuckDBPyConnection, table_name: str, parquet_dir: str) -> int:
    """Read all Parquet files in parquet_dir and load into DuckDB table."""
    pattern = os.path.join(parquet_dir, "**", "*.parquet")
    files = glob.glob(pattern, recursive=True)

    if not files:
        logger.warning("No Parquet files found in {} — skipping {}", parquet_dir, table_name)
        return 0

    # DuckDB can read a list of files or a glob pattern directly
    files_sql = ", ".join(f"'{f.replace(chr(92), '/')}'" for f in files)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet([{files_sql}])")
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info("Loaded {:,} rows into {}", count, table_name)
    return count


def main():
    if not os.path.exists(OUTPUT_DIR):
        logger.error(
            "output/ directory not found. Run the Spark streaming job first "
            "to generate the Parquet files."
        )
        return

    con = duckdb.connect(DB_PATH)

    raw_count     = load_table(con, "raw_transactions",     os.path.join(OUTPUT_DIR, "raw_transactions"))
    flagged_count = load_table(con, "flagged_transactions", os.path.join(OUTPUT_DIR, "flagged_transactions"))

    if raw_count == 0:
        logger.error("No raw transaction data found. Make sure Spark has written output files.")
        con.close()
        return

    # Quick sanity check
    logger.info("Schema of raw_transactions:")
    schema = con.execute("DESCRIBE raw_transactions").fetchdf()
    print(schema.to_string(index=False))

    logger.info("\nSample flagged transactions (top 5 by score):")
    sample = con.execute(
        "SELECT account_id, amount, fraud_score, fraud_reasons "
        "FROM flagged_transactions ORDER BY fraud_score DESC LIMIT 5"
    ).fetchdf()
    print(sample.to_string(index=False))

    con.close()
    logger.info("DuckDB database ready at: {}", os.path.abspath(DB_PATH))
    logger.info("Now run:  cd dbt && dbt run --profiles-dir .")


if __name__ == "__main__":
    main()
