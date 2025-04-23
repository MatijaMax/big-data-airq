import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b2"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Spark init
spark = SparkSession.builder \
    .appName("Get the global NO2 mean value") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

# Filtering
df_filtered = df.select("site_num", "date", "NO2_mean")

# We need only one site/date combination per local avg 
df_unique = df_filtered.dropDuplicates(["site_num", "date"])

# Average NO2 per site across unique dates
local_avgs = df_unique.groupBy("site_num").agg(avg("NO2_mean").alias("local_avg"))

# Global average across all sites
result = local_avgs.agg(avg("local_avg").alias("NO2_global_mean"))

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