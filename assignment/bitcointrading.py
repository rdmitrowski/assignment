#!/usr/bin/env python3
import sys
import argparse
import logging
import time
from datetime import datetime
from pyspark.sql.functions import concat, col, lit
from pyspark.sql import SparkSession, DataFrame
from logging.handlers import RotatingFileHandler

# from assignment.sess_init import get_spark_session


class BitcoinTrading:
    """
    This class with functions to load two files from filesystem located in input_data directory
    (user data and transaction data), modify it,
    join and save a result in client_data directory
    """

    def __init__(self):
        """
        Class initialization, creating log
        """
        self._log_initialize()
        # self._run_program_by_class_params(class_parameters)

    def get_spark_session(self):
        """
        get_spark_session
        Returns:
            sparkSession
        """
        # return get_spark_session()
        return (
            SparkSession.builder
            .appName("InitializeBitcoinTradingSparkSession")
            .getOrCreate()
        )

    def run_processing_flow(self, class_parameters: dict):
        """Run all processing flow for input parameters dict
        Args:
            class_parameters (dict): dictionary with {file_users, file_transactions, filter, file_output as optional}
        """
        self.logger.info(f"Initialized input parameters \
            {class_parameters['file_users']} \
            {class_parameters['file_transactions']} \
            {class_parameters['filter']} \
            {class_parameters['file_output']}")
        self.sparkSess = self.get_spark_session()
        df_users = self.load_file(f"input_data/{class_parameters['file_users']}")
        df_transactions = self.load_file(f"input_data/{class_parameters['file_transactions']}")
        df_users_filter = \
            self.dataset_filtering(df_users,
                                   f"""country in ("{class_parameters['filter']}")""")
        df_users_remove = self.column_remove(df_users_filter, "email")
        df_join = self.dataset_join(df_users_remove, df_transactions, 'id')
        df_users_new_cols = \
            self.column_create(
                df_join,
                "New name",
                concat(col("first_name"), lit(' '), col("last_name")))
        df_output = self.generate_output(df_users_new_cols,
                                         ['New name', 'id', 'btc_a', 'cc_t'])
        df_output = self.column_rename(df_output,
                                       {'id': 'client_identifier',
                                        'btc_a': 'bitcoin_address',
                                        'cc_t': 'credit_card_type'})
        self.write_output(df_output,
                          f"client_data/{class_parameters['file_output']}")

    def _log_initialize(self):
        """
        This function initialize rotating by size logs
        It's handle into file add into screen
        """
        log_handler = logging.handlers.RotatingFileHandler(
            filename='BitcoinTrading.log',
            mode='a',
            maxBytes=10**3*3,
            backupCount=5)
        formatter = logging.Formatter(
          '%(asctime)s BitcoinTrading [%(process)d]: [%(levelname)s] %(message)s',
          '%b %d %H:%M:%S')
        formatter.converter = time.gmtime
        log_handler.setFormatter(formatter)
        logger = logging.getLogger("BitcoinTrading rotating Log")
        logger.setLevel(logging.INFO)
        logger.addHandler(log_handler)
        streamHandler = logging.StreamHandler()
        streamFormat = formatter
        streamHandler.setFormatter(streamFormat)
        logger.addHandler(streamHandler)
        self.logger = logger

    def dataset_filtering(self, df: DataFrame, condition: str) -> DataFrame:
        """
        This function filters data in dataframe
        Input: Dataframe
               condition str
        Output: Dataframe
        """
        self.logger.info(f"BitcoinTrading.dataset_filtering: Output filtered by: {condition}")
        return df.filter(condition)

    def column_remove(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        This function removes column in dataframe
        Input: Dataframe
               column_name str
        Output: Dataframe
        """
        self.logger.info(f"Remove column from dataset: {column_name}")
        return df.drop(column_name)

    def column_rename(self, df: DataFrame, col_map: dict) -> DataFrame:
        """
        This function renames columns in dataframe
        Input: Dataframe
               col_map dict {old_name1: new_name1, old_name2, new_name2..}
        Output: Dataframe
        """
        for key, value in col_map.items():
            df = df.withColumnRenamed(key, value)
            self.logger.info(f"Rename column in dataset: {key} to {value}")
        return df

    def column_create(self, df: DataFrame, column_name: str, column_value: str) -> DataFrame:
        """
        This function creates new columns in dataframe
        Input: Dataframe
               column name str
               column value str
        Output: Dataframe
        """
        self.logger.info(f"Create column in dataset: {column_name} as {column_value}")
        return df.withColumn(column_name, column_value)

    def dataset_join(self, df1: DataFrame, df2: DataFrame, join_key: str) -> DataFrame:
        """
        This function joins two dataframes
        Input: Dataframe
               Dataframe
               join_key str
        Output: Dataframe
        """
        self.logger.info("Join two datasets")
        return df1.join(df2, on=join_key, how="inner")

    def generate_output(self, df: DataFrame, column_list: list) -> DataFrame:
        """
        This function generates dataframe with specific column listed
        Input: Dataframe
               column_list list
        Output: Dataframe
        """
        self.logger.info("Generate output")
        return df.select(column_list)

    def write_output(self, df: DataFrame, file_output: str):
        """
        This function save dataframe as a file in output_location (client_data)
        Input: Datafram
        Output: file_output str - file name
        """
        try:
            self.logger.info("Trying to save a file into filesystem")
            df.write.format("csv").mode("overwrite").option("header", "true").save(file_output)
            self.logger.info(f"File {file_output} saved in filesystem with {str(df.count())} row(s)")
        except IOError as IOerr:
            self.logger.error(f"Problem with saving {file_output}")
            self.logger.error(IOerr)
        except Exception as e:
            self.logger.error(e)

    def load_file(self, file_name: str) -> DataFrame:
        """
        This function loads file into dataframe from input_location (input_data)
        Input: file_name str
        Output: Dataframe
        """
        file_type = "csv"
        infer_schema = "false"
        first_row_is_header = "true"
        delimiter = ","
        self.logger.info(f"Loading file: {file_name}")
        try:
            df = self.sparkSess.read.format(file_type) \
                .option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(file_name)
            self.logger.info(f'Loaded {str(df.count())} from file_name')
            return df
        except FileNotFoundError as err:
            self.logger.error(f"Problem with loading {file_name}, file doesn't exists")
            self.logger.error(err)
            raise
        except IOError as IOerr:
            self.logger.error(f"Problem with loading {file_name}, IOError")
            self.logger.error(err)
            raise
        except Exception as e:
            self.logger.error(e)
            raise
