"""
conftest.py

Shared fixtures for the test suite. The main thing here is the SparkSession —
creating it is slow (~10s), so scope=session means it's created once and reused
across all test files. If a test leaves the session in a bad state, that's a bug
in the test, not a reason to use a narrower scope.

The ui.enabled=false is important — without it, Spark spins up a web server on
port 4040 and slows down test startup for no reason.

Python 3.14 + PySpark 3.5 needs a higher recursion limit — PySpark's pickle
serializer hits the default limit when serializing Spark functions on 3.14.
"""

import sys
import pytest
from pyspark.sql import SparkSession

# PySpark's closure serializer recurses deeply on Python 3.14
sys.setrecursionlimit(10000)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("fraud-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
