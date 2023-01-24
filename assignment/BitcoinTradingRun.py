import sys
from pyspark.sql.functions import concat, col, lit
from pyspark.sql import SparkSession
import BitcoinTrading

NewBitcoinTradingClass = BitcoinTrading()
NewBitcoinTradingClass.file_info_logging('Starting ' + str(sys.argv[0]) )
NewBitcoinTradingClass.check_parameters()
NewBitcoinTradingClass.file_info_logging('Initialized input parameters ' + str(sys.argv[0]) + ' ' + NewBitcoinTradingClass.file_users_location + ' ' + NewBitcoinTradingClass.file_transactions_location + ' ' + NewBitcoinTradingClass.countries + ' ' + NewBitcoinTradingClass.file_output )
  
spark = SparkSession.builder.appName("BitcoinTrading").getOrCreate()
#NewBitcoinTradingClass.file_info_logging("Loading: " + NewBitcoinTradingClass.file_users_location)
NewBitcoinTradingClass.load_file( NewBitcoinTradingClass.file_users_location, 0)

#NewBitcoinTradingClass.file_info_logging("Loading: " + NewBitcoinTradingClass.file_transactions_location)
NewBitcoinTradingClass.load_file( NewBitcoinTradingClass.file_transactions_location, 1)

NewBitcoinTradingClass.dataset_filtering("country in ('" + NewBitcoinTradingClass.countries + "')")

NewBitcoinTradingClass.column_remove("email")
NewBitcoinTradingClass.column_rename("id","client_identifier")
NewBitcoinTradingClass.column_create("New name", concat(col("first_name"), lit(' '), col("last_name")))
NewBitcoinTradingClass.column_remove("first_name")
NewBitcoinTradingClass.column_remove("last_name")

NewBitcoinTradingClass.column_remove_tr("cc_n")

NewBitcoinTradingClass.dataset_join()
NewBitcoinTradingClass.generate_output()
NewBitcoinTradingClass.write_output()