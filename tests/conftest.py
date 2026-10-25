import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Creates a single SparkSession for the entire test suite."""
    spark_session = SparkSession.builder \
        .appName("Pytest-PySpark") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()
