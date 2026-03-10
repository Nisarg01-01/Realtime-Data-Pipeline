"""
Pytest configuration file that sets up the environment for all tests.
This ensures PySpark and other dependencies are properly configured on any system.
"""

import os
import sys
import subprocess
import pytest


def pytest_configure(config):
    """
    Hook that runs before test collection.
    Sets up environment variables needed for PySpark on all platforms.
    """
    # Get the Python executable path from the current environment
    python_executable = sys.executable
    
    # Set environment variables for PySpark (critical for Windows compatibility)
    os.environ['PYSPARK_PYTHON'] = python_executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable
    
    # Additional environment variables for better PySpark stability
    os.environ['PYSPARK_PIN_THREAD'] = 'true'
    
    print("\n[OK] PySpark environment configured:")
    print(f"  PYSPARK_PYTHON = {os.environ['PYSPARK_PYTHON']}")
    print(f"  PYSPARK_DRIVER_PYTHON = {os.environ['PYSPARK_DRIVER_PYTHON']}")
    print()


@pytest.fixture(scope="session")
def spark_session():
    """
    Fixture that provides a configured Spark session for all tests.
    Ensures proper configuration to avoid worker connection issues.
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("PytestSparkSession") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.rdd.compress", "false") \
        .config("spark.shuffle.compress", "false") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.memory", "512m") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "256m") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()
