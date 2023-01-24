import pyspark
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit
#from ..project1.BitcoinTrading import BitcoinTrading
from ..project1.BitcoinTrading import BitcoinTrading

#class Test_bitcointrading

def test_dataset_filtering():
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10,"a", 1), (50, "e", 5)]
   expected_columns = ["id", "seq_number", "value"]
   NewBitcoinTrading = BitcoinTrading()
   
   NewBitcoinTrading.df_users = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = NewBitcoinTrading.sparkSess.createDataFrame(data=expected_data, schema=expected_columns)   
   NewBitcoinTrading.dataset_filtering("id in (10,50)")
   assert_df_equality(NewBitcoinTrading.df_users, df_expected)

def test_column_remove():
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10, 1), (20, 2), (30, 3), (40,4), (50,5)]
   expected_columns = ["id", "value"]
   NewBitcoinTrading = BitcoinTrading()
   
   NewBitcoinTrading.df_users = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = NewBitcoinTrading.sparkSess.createDataFrame(data=expected_data, schema=expected_columns)   
   NewBitcoinTrading.column_remove("seq_number")
   assert_df_equality(NewBitcoinTrading.df_users, df_expected)

def test_column_remove_tr():
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10, 1), (20, 2), (30, 3), (40,4), (50,5)]
   expected_columns = ["id", "value"]
   NewBitcoinTrading = BitcoinTrading()
   
   NewBitcoinTrading.df_transactions = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = NewBitcoinTrading.sparkSess.createDataFrame(data=expected_data, schema=expected_columns)   
   NewBitcoinTrading.column_remove_tr("seq_number")
   assert_df_equality(NewBitcoinTrading.df_transactions, df_expected)

def test_column_rename():
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   expected_columns = ["id", "incemental_id", "value"]
   NewBitcoinTrading = BitcoinTrading()
   
   NewBitcoinTrading.df_users = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = NewBitcoinTrading.sparkSess.createDataFrame(data=expected_data, schema=expected_columns)   
   NewBitcoinTrading.column_rename("seq_number", "incemental_id")
   assert_df_equality(NewBitcoinTrading.df_users, df_expected)
   
def test_column_create():   
   input_data = [(10,"a", 1), (20,"b", 2), (30, "c", 3), (40, "d", 4), (50, "e", 5)]
   input_columns = ["id", "seq_number", "value"]
   expected_data = [(10,"a", 1, "10 1"), (20,"b", 2, "20 2"), (30, "c", 3, "30 3"), (40, "d", 4, "40 4"), (50, "e", 5, "50 5")]
   expected_columns = ["id", "seq_number", "value", "multiplication"]
   NewBitcoinTrading = BitcoinTrading()
   
   NewBitcoinTrading.df_users = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data, schema=input_columns) 
   df_expected = NewBitcoinTrading.sparkSess.createDataFrame(data=expected_data, schema=expected_columns)   
   NewBitcoinTrading.column_create("multiplication", concat(col("id"), lit(' '), col("value")))
   assert_df_equality(NewBitcoinTrading.df_users, df_expected)
   
def test_dataset_join():
   input_data1 = [(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)]
   input_columns1 = ["val", "client_identifier"]
   input_data2 = [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)]
   input_columns2 = ["literal", "id"]
   
   expected_data = [(10, 1, "a", 1), (20, 2, "b", 2), (30,  3, "c", 3), (40,  4, "d", 4), (50,  5, "e", 5)]
   expected_columns = ["val", "client_identifier", "literal", "id"]
   NewBitcoinTrading = BitcoinTrading()
   
   NewBitcoinTrading.df_users = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data1, schema=input_columns1)
   NewBitcoinTrading.df_transactions = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data2, schema=input_columns2)
   df_expected = NewBitcoinTrading.sparkSess.createDataFrame(data=expected_data, schema=expected_columns)
   NewBitcoinTrading.dataset_join()
   assert_df_equality(NewBitcoinTrading.df_join, df_expected)
   

def test_generate_output():
   input_data = [(10, 1, "a", 1, 7), (20, 2, "b", 2, 9), (30,  3, "c", 3, 28), (40,  4, "d", 4, 55), (50,  5, "e", 5, 99)]
   input_columns = ["val", "New name", "client_identifier", "btc_a", "cc_t"]
   expected_data = [(1, "a", 1, 7), (2, "b", 2, 9), (3, "c", 3, 28), (4, "d", 4, 55), (5, "e", 5, 99)]
   expected_columns = ["New name", "client_identifier", "bitcoin_address", "credit_card_type"]
   NewBitcoinTrading = BitcoinTrading()
   
   NewBitcoinTrading.df_join = NewBitcoinTrading.sparkSess.createDataFrame(data=input_data, schema=input_columns)
   df_expected = NewBitcoinTrading.sparkSess.createDataFrame(data=expected_data, schema=expected_columns)
   NewBitcoinTrading.generate_output()
   assert_df_equality(NewBitcoinTrading.df_output, df_expected)

def test_load_file():
   pass