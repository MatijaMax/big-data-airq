import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count, desc, row_number

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b7"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Spark init
spark = SparkSession.builder \
    .appName("Find the most common hour where the maximum measured values occurred in each county") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)\

# Group by county and see how often each hour occurs
df_no2 = df.groupBy("county", "NO2_hour_max").agg(count("*").alias("max_hour_count_no2"))
df_o3 = df.groupBy("county", "O3_hour_max").agg(count("*").alias("max_hour_count_o3"))
df_so2 = df.groupBy("county", "SO2_hour_max").agg(count("*").alias("max_hour_count_so2"))
df_co = df.groupBy("county", "CO_hour_max").agg(count("*").alias("max_hour_count_co"))


# Find the hour with the highest number per county
window_no2 = Window.partitionBy("county").orderBy(desc("max_hour_count_no2"))
most_common_no2 = df_no2.withColumn("rn", row_number().over(window_no2)).filter(col("rn") == 1).drop("rn")

window_o3 = Window.partitionBy("county").orderBy(desc("max_hour_count_o3"))
most_common_o3 = df_o3.withColumn("rn", row_number().over(window_o3)).filter(col("rn") == 1).drop("rn")

window_so2 = Window.partitionBy("county").orderBy(desc("max_hour_count_so2"))
most_common_so2 = df_so2.withColumn("rn", row_number().over(window_so2)).filter(col("rn") == 1).drop("rn")

window_co = Window.partitionBy("county").orderBy(desc("max_hour_count_co"))
most_common_co = df_co.withColumn("rn", row_number().over(window_co)).filter(col("rn") == 1).drop("rn")


# Join all
result = most_common_no2 \
    .join(most_common_o3, on="county", how="outer") \
    .join(most_common_so2, on="county", how="outer") \
    .join(most_common_co, on="county", how="outer")

# Show result
result.show()

# Save data
result.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

# Stop 
spark.stop()