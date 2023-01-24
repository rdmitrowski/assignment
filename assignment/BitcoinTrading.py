#!/usr/bin/env python3
import sys
from pyspark.sql.functions import concat, col, lit
from pyspark.sql import SparkSession
import argparse

class BitcoinTrading:
  """This class load two files from filesystem from input_data directory (user data and transaction data), modify it, join and save a result in client_data directory""" 

  def __init__(self, file_users=None, file_transactions=None, filter=None):
    self._log_initialize()
    self.sparkSess = SparkSession.builder.appName("InitializeBitcoinTradingSparkSession ").getOrCreate()
    if file_users != None and file_transactions!= None and filter != None:
      self.file_users= file_users
      self.file_transactions= file_transactions
      self.filter= filter
      self._run_program_by_class_params()
     
  def _run_program_by_class_params(self):
        from datetime import datetime
        now = datetime.now()
        print(self.file_users, self.file_transactions, self.filter)
        file_output= 'output_data_' + now.strftime("%d-%m-%Y_%H%M%S") + '.csv'
     
        self.file_info_logging(f'Initialized input parameters {self.file_users} {self.file_transactions} {self.filter}' )
      
        self.load_file( f'input_data/{self.file_users}', 0)
        self.load_file( f'input_data/{self.file_transactions}', 1)
        self.dataset_filtering(f"country in ('{self.filter}')")
        self.column_remove("email")
        self.column_rename("id","client_identifier")
        self.column_create("New name", concat(col("first_name"), lit(' '), col("last_name")))
        self.column_remove("first_name")
        self.column_remove("last_name")
        self.column_remove_tr("cc_n")
        self.dataset_join()
        self.generate_output()
        self.write_output(f'client_data/{file_output}')
     
  def file_info_logging(self, val:str):  
    self.logger.info( val )
   
  def  file_warning_logging(self, val:str):  
    self.logger.warning( val )

  def file_error_logging(self, val:str):
    self.logger.error( val )

  def _check_parameters(self):
    """Function validate if we are running code with proper number of attributes
    Proper parameter for running purpose is:
    BitcoinTrading.py <users_input_file> <transactions_input_file> <country_filter>
    """
    if len(self.params) in (4,5):
     # File location and type
     self.file_info_logging('BitcoinTrading.check_parameters: Validating input parameters')
     self.file_users_location = """input_data/""" + str(self.params[1])
     self.file_transactions_location = """input_data/""" + str(self.params[2])
     self.countries = str(self.params[3])
     if len(self.params) == 5:
       self.file_output = """client_data/""" + str(self.params[4]) + '.csv'
     else:
       self.file_output = """client_data/output_data"""
       from datetime import datetime
       now = datetime.now()
       self.file_output = self.file_output +'_' + now.strftime("%d-%m-%Y_%H%M%S") + '.csv' 
       self.file_info_logging('BitcoinTrading.check_parameters: Parameters correctly parsed')
    else:
     self.logger.error('Incorrect input parameters')
     self.logger.error('usage: python3 BitcoinTrading.py <users_input> <transactions_input> <country>')
     print("usage: python3 BitcoinTrading.py <users_input> <transactions_input> <country>")
     exit(0)
    # return file_users_location, file_transactions_location, countries, file_output
  
  def _log_initialize(self):
    import logging
    import time
    from logging.handlers import RotatingFileHandler
  
    log_handler = logging.handlers.RotatingFileHandler(filename='BitcoinTrading.log', mode='a', maxBytes=10**3*3, backupCount=5)
    formatter = logging.Formatter(
        '%(asctime)s BitcoinTrading [%(process)d]: [%(levelname)s] %(message)s',
        '%b %d %H:%M:%S')
    formatter.converter = time.gmtime  # if you want UTC time
    log_handler.setFormatter(formatter)
    logger = logging.getLogger("BitcoinTrading rotating Log")
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handler)
    #stream handler
    streamHandler = logging.StreamHandler()
    streamFormat = formatter
    streamHandler.setFormatter(streamFormat)
    logger.addHandler(streamHandler)

    #return logger
    self.logger = logger

  def dataset_filtering(self, condition:str):
    self.file_info_logging(f"BitcoinTrading.dataset_filtering: Output filtered by: {condition}")
    self.df_users = self.df_users.filter(condition)
    
  def column_remove(self, column_name:str):
    self.file_info_logging(f"Remove column from dataset: {column_name}")
    self.df_users = self.df_users.drop(column_name)

  def column_remove_tr(self, column_name:str):
    self.file_info_logging(f"Remove column rom dataset: {column_name}")
    self.df_transactions = self.df_transactions.drop(column_name)    
    
  def column_rename(self, old_column_name:str, new_column_name:str):
    self.file_info_logging(f"Rename column in dataset: {old_column_name} to {new_column_name}")
    self.df_users = self.df_users.withColumnRenamed(old_column_name, new_column_name)
  
  def column_create(self, column_name:str, column_value:str):
    self.file_info_logging(f"Create column in dataset: {column_name} as {column_value}")
    self.df_users = self.df_users.withColumn(column_name, column_value)
    
  def dataset_join(self):
    self.file_info_logging("Join two datasets")
    self.df_join = self.df_users.join(self.df_transactions, self.df_users.client_identifier== self.df_transactions.id, "inner")
  
  def generate_output(self):
    self.file_info_logging("Generate output")
    self.df_output = self.df_join.select("New name", "client_identifier",col("btc_a").alias("bitcoin_address"),col("cc_t").alias("credit_card_type"))

  def write_output(self, file_output:str):
    try:
      self.file_info_logging("Trying to save a file into filesystem")
      self.df_output.write.format("csv").mode("overwrite").option("header", "true").save(file_output)
      self.file_info_logging(f"File {file_output} saved in filesystem with {str(self.df_output.count())} row(s)")
    except IOError as IOerr:
      self.file_error_logging(f"Problem with saving {file_output}")
      self.file_error_logging(IOerr)
    except Exception as e:
      self.file_error_logging(e)  

  def load_file(self, file_name:str, level:int):
    file_type = "csv"
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","
    
    self.file_info_logging(f"Loading file: {file_name}")
    
    if level == 0: #users
      try: 
        self.df_users = self.sparkSess.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_name)
        self.file_info_logging(f'Loaded {str(self.df_users.count())} users')
      except  FileNotFoundError as err:
        self.file_error_logging(f"Problem with loading {file_name}")
        self.file_error_logging(err)
      except IOError as IOerr:
        self.file_error_logging(f"Problem with loading {file_name}")
        self.file_error_logging(err)
      except Exception as e:
        self.file_error_logging(e)  

  
    elif level == 1: #transaction
      try: 
        self.df_transactions = self.sparkSess.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_name)
        self.file_info_logging(f'Loaded {str(self.df_transactions.count())} users')
      except  FileNotFoundError as err:
        self.file_error_logging(f"Problem with loading {file_name}")
        self.file_error_logging(err)
      except IOError as IOerr:
        self.file_error_logging(f"Problem with loading {file_name}")
        self.file_error_logging(err)
      except Exception as e:
        self.file_error_logging(e)

def main():
  NewBitcoinTradingClass = BitcoinTrading()
  from datetime import datetime
  now = datetime.now()
   
  parser = argparse.ArgumentParser(description='Program to parse two dataset, filter, join and return output')
  parser.add_argument('-file_users', type=str, help='Name of users file', required=True)
  parser.add_argument('-file_transactions', type=str, help='Name of transaction file', required=True)
  parser.add_argument('-filter', type=str, help='Country filter', required=True)
  parser.add_argument('-file_output', type=str, help='Optional output file name', required=False, default='output_data_' + now.strftime("%d-%m-%Y_%H%M%S") + '.csv')
  
  NewBitcoinTradingClass.params = parser.parse_args()
  
  NewBitcoinTradingClass.file_info_logging('Initialized input parameters {NewBitcoinTradingClass.params.file_users} {NewBitcoinTradingClass.params.file_transactions} {NewBitcoinTradingClass.params.filter}' )

  NewBitcoinTradingClass.load_file( f'input_data/{NewBitcoinTradingClass.params.file_users}', 0)
  NewBitcoinTradingClass.load_file( f'input_data/{NewBitcoinTradingClass.params.file_transactions}', 1)
  NewBitcoinTradingClass.dataset_filtering(f"country in ('{NewBitcoinTradingClass.params.filter}')")
  NewBitcoinTradingClass.column_remove("email")
  NewBitcoinTradingClass.column_rename("id","client_identifier")
  NewBitcoinTradingClass.column_create("New name", concat(col("first_name"), lit(' '), col("last_name")))
  NewBitcoinTradingClass.column_remove("first_name")
  NewBitcoinTradingClass.column_remove("last_name")
  NewBitcoinTradingClass.column_remove_tr("cc_n")
  NewBitcoinTradingClass.dataset_join()
  NewBitcoinTradingClass.generate_output()
  NewBitcoinTradingClass.write_output(f'client_data/{NewBitcoinTradingClass.params.file_output}')

if __name__ == '__main__':
  main()        