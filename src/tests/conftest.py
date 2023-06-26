import pytest
from pyspark.sql import SparkSession


@pytest.fixture(name='spark_session', scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("sparktest").getOrCreate()
    yield spark
    spark.stop()
