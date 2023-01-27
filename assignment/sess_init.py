from pyspark.sql import SparkSession

def get_spark_session():
    """
    get_spark_session
    Returns:
        sparkSession
    """
    return SparkSession.builder \
        .appName("InitializeBitcoinTradingSparkSession") \
        .getOrCreate()
