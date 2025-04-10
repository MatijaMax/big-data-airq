import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

# Spark init
spark = SparkSession.builder \
    .appName("AIRQ CLEANER") \
    .getOrCreate()

# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
RAW_DATA_PATH = HDFS_NAMENODE + "/raw/"
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read raw data
df_raw = spark.read.option("header", True).csv(RAW_DATA_PATH)

# Rename columns
renames = {
    "Site Num": "site_num",
    "Address": "address",
    "State": "state",
    "County": "county",
    "City": "city",
    "Date Local": "date",
    "NO2 Mean": "NO2_mean",
    "NO2 1st Max Value": "NO2_day_max",
    "NO2 1st Max Hour": "NO2_hour_max",     
    "NO2 AQI": "NO2_AQI",
    "O3 Mean": "O3_mean",
    "O3 1st Max Value": "O3_day_max",
    "O3 1st Max Hour": "O3_hour_max",     
    "O3 AQI": "O3_AQI",    
    "SO2 Mean": "SO2_mean",
    "SO2 1st Max Value": "SO2_day_max",
    "SO2 1st Max Hour": "SO2_hour_max",     
    "SO2 AQI": "SO2_AQI",
    "CO Mean": "CO_mean",
    "CO 1st Max Value": "CO_day_max",
    "CO 1st Max Hour": "CO_hour_max",     
    "CO AQI": "CO_AQI",           
}
for old, new in renames.items():
    df_raw = df_raw.withColumnRenamed(old, new)

# Remove columns
columns_to_keep = list(renames.values())
df_cleaned = df_raw.select(columns_to_keep)

# Remove rows with no values
required_columns = [
    "NO2_mean", "NO2_day_max", "NO2_hour_max", "NO2_AQI",
    "O3_mean", "O3_day_max", "O3_hour_max", "O3_AQI",
    "SO2_mean", "SO2_day_max", "SO2_hour_max", "SO2_AQI",
    "CO_mean", "CO_day_max", "CO_hour_max", "CO_AQI"
]

df_cleaned = df_cleaned.dropna(subset=required_columns)

# Convert date data types
df_cleaned = df_cleaned.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))\

# Convert numeric data types
pollutant_columns = [
    "NO2_mean", "NO2_day_max", "NO2_hour_max", "NO2_AQI",
    "O3_mean", "O3_day_max", "O3_hour_max", "O3_AQI",
    "SO2_mean", "SO2_day_max", "SO2_hour_max", "SO2_AQI",
    "CO_mean", "CO_day_max", "CO_hour_max", "CO_AQI"
]

for col_name in pollutant_columns:
    df_cleaned = df_cleaned.withColumn(col_name, col(col_name).cast(DoubleType()))

# Save cleaned data
df_cleaned.write.mode("overwrite").parquet(TRANSFORMATION_DATA_PATH)

# Stop stop
spark.stop()