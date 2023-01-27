import pytest
from pyspark.sql import DataFrame
from ..assignment.sess_init import get_spark_session

spark = get_spark_session()


@pytest.fixture
def dataset_filtering_data() -> DataFrame:
    input_data = [(10, "a", 1), (20, "b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
    input_columns = ["id", "seq_number", "value"]
    return spark.createDataFrame(input_data, input_columns)
