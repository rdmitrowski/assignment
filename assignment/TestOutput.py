import sys
from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
print("**************************")
print("Starting tests validation ")

if len(sys.argv) == 3:
 # File location and type
 file_output   = """client_data/""" + str(sys.argv[1])
 file_expected = """client_data/""" + str(sys.argv[2])
else:
 print("Incorrect parameters. Use: TestOutput.py <output_file> <expected_file>")
 exit(0)

# CSV options
file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

spark = SparkSession.builder.appName("TestOutput").getOrCreate()

df_output = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_output)

df_expected = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_expected)

assert_df_equality(df_output, df_expected)

print("Tests validation success")
print("**************************")
