import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b3"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Spark init
spark = SparkSession.builder \
    .appName("List the top 5 cities with the highest AQI for O3") \
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
df_filtered = df.select("city", "site_num", "date", "O3_AQI")
df_filtered = df_filtered.filter(col("city") != "Not in a city")

# We need only one site/date combination per local avg 
df_unique = df_filtered.dropDuplicates(["site_num", "date"])

# Average O3 AQI per site across unique dates
local_avgs = df_unique.groupBy("site_num", "city").agg(avg("O3_AQI").alias("local_avg"))

# 5 cities with the highest AQI levels
result = local_avgs.groupBy("city").agg(avg("local_avg").alias("O3_AQI")).limit(5) \
    .orderBy(col("O3_AQI").desc()) \
    .limit(5)

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