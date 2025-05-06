import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, month, dayofmonth, col

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b5"

# Spark init
spark = SparkSession.builder \
    .appName("Identify cities with highest combined AQI in winter") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)\

# Filtering
df_filtered = df.select("state", "city", "site_num", "date", "NO2_AQI", "O3_AQI", "SO2_AQI", "CO_AQI")

# Drop duplicates
df_unique = df_filtered.dropDuplicates(["state", "city", "site_num", "date"])

# Filter winter months(December, January, February)
df_winter = df_unique.filter(
    ( (month("date") == 12) & (dayofmonth("date") >= 21) ) |
    (month("date") == 1) |
    (month("date") == 2) |
    ( (month("date") == 3) & (dayofmonth("date") <= 20) )
)

# Combined AQI
df_winter = df_winter.withColumn(
    "combined_AQI",
    (col("NO2_AQI") + col("O3_AQI") + col("SO2_AQI") + col("CO_AQI"))
)

city_avg = df_winter.groupBy("city").agg(
    avg("combined_AQI").alias("avg_combined_AQI_in_winter")
)
result = city_avg.filter(col("city") != "Not in a city")

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