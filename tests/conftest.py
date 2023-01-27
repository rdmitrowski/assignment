import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def dataset_filtering_data():
    return SparkSession.builder \
        .appName("InitializeBitcoinTradingSparkSession") \
        .getOrCreate()
