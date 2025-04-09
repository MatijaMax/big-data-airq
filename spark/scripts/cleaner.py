import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import DoubleType, IntegerType, BooleanType

# Spark init
spark = SparkSession.builder \
    .appName("AIRQ CLEANER") \
    .getOrCreate()

# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
RAW_DATA_PATH = HDFS_NAMENODE + "/"
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/"

# Read raw data
df_raw = spark.read.option("header", True).csv(RAW_DATA_PATH)

# Rename columns

# Remove columns

# Remove rows with no values

# Convert data type if needed

# Convert numeric data types

# Convert boolean types

# Save cleaned data

# Stop stop
spark.stop()