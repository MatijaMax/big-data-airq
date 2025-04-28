import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, year, month

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b6"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Spark init
spark = SparkSession.builder \
    .appName("Show how highest measured daily pollutant values changed over time") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)\

df = df.withColumn("year", year("date")) \
    .withColumn("month", month("date"))

df_daily_max = df.select("year", "month", "date", "NO2_day_max", "O3_day_max", "SO2_day_max", "CO_day_max") 

result = df_daily_max.groupBy("year", "month").agg(
    avg("NO2_day_max").alias("NO2_day_max"),
    avg("O3_day_max").alias("O3_day_max"),
    avg("SO2_day_max").alias("SO2_day_max"),
    avg("CO_day_max").alias("CO_day_max")
).orderBy("year", "month")

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