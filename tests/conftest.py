import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("InitializeBitcoinTradingSparkSession")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark_session.sparkContext.stop())
    return spark_session
