import pytest
from pyspark.sql import SparkSession

@pytest.fixture(name='spark_session')
def fixture_spark_session():
    spark = SparkSession.builder.master("local[*]").appName("chispa").getOrCreate()
    yield spark
    spark.stop()
