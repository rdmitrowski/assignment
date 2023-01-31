import pyspark
from chispa import assert_df_equality
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat, col, lit
from assignment.bitcointrading import BitcoinTrading
import sys

TEST_DATA = [(10, "a", 1), (20, "b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
TEST_COLUMNS = ["id", "seq_number", "value"]

bt = BitcoinTrading()


def load_dataframe(spark_session) -> DataFrame:
    return spark_session.createDataFrame(data=TEST_DATA, schema=TEST_COLUMNS)


def test_dataset_filtering(spark_session):
    print(f'Validation {sys.path}')
    expected_data = [(10, "a", 1), (50, "e", 5)]
    expected_columns = ["id", "seq_number", "value"]
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(bt.dataset_filtering(load_dataframe(spark_session), "id in (10, 50)"), df_expected)


def test_column_remove(spark_session):
    expected_data = [(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)]
    expected_columns = ["id", "value"]
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(bt.column_remove(load_dataframe(spark_session), "seq_number"), df_expected)


def test_column_rename(spark_session):
    expected_data = [(10, "a", 1), (20, "b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
    expected_columns = ["id", "incemental_id", "value"]
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(bt.column_rename(load_dataframe(spark_session), {'seq_number': 'incemental_id'}), df_expected)


def test_column_create(spark_session):
    expected_data = [(10, "a", 1, "10 1"), (20, "b", 2, "20 2"), (30, "c", 3, "30 3"), (40, "d", 4, "40 4"), (50, "e", 5, "50 5")]
    expected_columns = ["id", "seq_number", "value", "multiplication"]
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(bt.column_create(load_dataframe(spark_session), "multiplication", concat(col("id"), lit(' '), col("value"))), df_expected)


def test_dataset_join(spark_session):
    input_data1 = [(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)]
    input_columns1 = ["val", "id"]
    input_data2 = [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)]
    input_columns2 = ["literal", "id"]

    expected_data = [(1, 10, "a"), (2, 20, "b"), (3, 30,  "c"), (4, 40,  "d"), (5, 50, "e")]
    expected_columns = ["id", "val", "literal"]

    df_users = spark_session.createDataFrame(data=input_data1, schema=input_columns1)
    df_transactions = spark_session.createDataFrame(data=input_data2, schema=input_columns2)
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(bt.dataset_join(df_users, df_transactions, 'id'), df_expected)


def test_generate_output(spark_session):
    input_data = [(10, 1, "a", 1, 7), (20, 2, "b", 2, 9), (30,  3, "c", 3, 28), (40,  4, "d", 4, 55), (50,  5, "e", 5, 99)]
    input_columns = ["val", "New name", "id", "btc_a", "cc_t"]
    expected_data = [(1, "a", 1, 7), (2, "b", 2, 9), (3, "c", 3, 28), (4, "d", 4, 55), (5, "e", 5, 99)]
    expected_columns = ["New name", "id", "btc_a", "cc_t"]

    df_join = spark_session.createDataFrame(data=input_data, schema=input_columns)
    df_expected = spark_session.createDataFrame(data=expected_data, schema=expected_columns)
    assert_df_equality(bt.generate_output(df_join, ["New name", "id", "btc_a", "cc_t"]), df_expected)
