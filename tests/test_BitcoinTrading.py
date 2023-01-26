import pyspark
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit
from ..assignment.BitcoinTrading import BitcoinTrading

bt = BitcoinTrading()
spark = bt.get_spark_session()

def test_dataset_filtering():
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10,"a", 1), (50, "e", 5)]
   expected_columns = ["id", "seq_number", "value"]
   
   df_users = spark.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = spark.createDataFrame(data=expected_data, schema=expected_columns)   
   assert_df_equality(bt.dataset_filtering(df_users, "id in (10,50)"), df_expected)

def test_column_remove():
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10, 1), (20, 2), (30, 3), (40,4), (50,5)]
   expected_columns = ["id", "value"]
   df_users = spark.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = spark.createDataFrame(data=expected_data, schema=expected_columns)   
   assert_df_equality(bt.column_remove(df_users, "seq_number"), df_expected)

def test_column_rename():
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   expected_columns = ["id", "incemental_id", "value"]
   df_users = spark.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = spark.createDataFrame(data=expected_data, schema=expected_columns)
   assert_df_equality(bt.column_rename(df_users, {'seq_number': 'incemental_id'}), df_expected)

def test_column_create():   
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10,"a", 1, "10 1"), (20,"b", 2, "20 2"), (30, "c", 3, "30 3"), (40, "d", 4, "40 4"), (50, "e", 5, "50 5")]
   expected_columns = ["id", "seq_number", "value", "multiplication"]
   df_users = spark.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = spark.createDataFrame(data=expected_data, schema=expected_columns)   
   assert_df_equality( bt.column_create(df_users, "multiplication", concat(col("id"), lit(' '), col("value"))), df_expected)
   
def test_dataset_join():
   input_data1 = [(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)]
   input_columns1 = ["val", "id"]
   input_data2 = [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)]
   input_columns2 = ["literal", "id"]
   
   expected_data = [(1, 10, "a"), (2, 20, "b"), (3, 30,  "c"), (4, 40,  "d"), (5, 50, "e")]
   expected_columns = ["id", "val", "literal"]
   
   df_users = spark.createDataFrame(data=input_data1, schema=input_columns1)
   df_transactions = spark.createDataFrame(data=input_data2, schema=input_columns2)
   df_expected = spark.createDataFrame(data=expected_data, schema=expected_columns)
   assert_df_equality( bt.dataset_join(df_users, df_transactions, 'id'), df_expected)

def test_generate_output():
   input_data = [(10, 1, "a", 1, 7), (20, 2, "b", 2, 9), (30,  3, "c", 3, 28), (40,  4, "d", 4, 55), (50,  5, "e", 5, 99)]
   input_columns = ["val", "New name", "id", "btc_a", "cc_t"]
   expected_data = [(1, "a", 1, 7), (2, "b", 2, 9), (3, "c", 3, 28), (4, "d", 4, 55), (5, "e", 5, 99)]
   expected_columns = ["New name", "id", "btc_a", "cc_t"]
   df_join = spark.createDataFrame(data=input_data, schema=input_columns)
   df_expected = spark.createDataFrame(data=expected_data, schema=expected_columns)
   assert_df_equality(bt.generate_output(df_join, ["New name", "id", "btc_a", "cc_t" ]), df_expected)

def test_load_file():
   pass