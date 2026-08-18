"""
Quick script to read and display the fraud-scored output from the Spark job.
Run from the project root:
    python read_output.py
"""
import os
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\ushak\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("read-output")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*60)
print("FLAGGED TRANSACTIONS (fraud_score > 0)")
print("="*60)
flagged = spark.read.parquet("output/flagged_transactions")
print(f"Total flagged records: {flagged.count()}")
flagged.select("account_id", "amount", "fraud_score", "fraud_reasons").show(20, truncate=False)

print("\n" + "="*60)
print("ALL TRANSACTIONS (sample)")
print("="*60)
raw = spark.read.parquet("output/raw_transactions")
print(f"Total records: {raw.count()}")
raw.select("account_id", "amount", "fraud_score", "is_anomalous").show(10, truncate=False)

spark.stop()
