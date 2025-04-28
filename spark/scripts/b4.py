import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b4"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Spark init
spark = SparkSession.builder \
    .appName("What is the average AQI for pollutants (NO2, O3, SO2, CO) in each state") \
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
df_filtered = df.select("state", "site_num", "date", "NO2_AQI", "O3_AQI", "SO2_AQI", "CO_AQI")

# We need only one site/date combination per local avg 
df_unique = df_filtered.dropDuplicates(["site_num", "date"])

# Average O3 AQI per state 
result = df_unique.groupBy("state").agg(
    avg("NO2_AQI").alias("avg_NO2_AQI"),
    avg("O3_AQI").alias("avg_O3_AQI"),
    avg("SO2_AQI").alias("avg_SO2_AQI"),
    avg("CO_AQI").alias("avg_CO_AQI")
).orderBy("state")

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