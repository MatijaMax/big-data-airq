import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import stddev, row_number, desc, col, avg, count, isnan, when

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b8"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Spark init
spark = SparkSession.builder \
    .appName("Rank cities based on the standard variation of measured daily AQI values using a sliding window") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)
# Sliding window (30 reads)
window_spec = Window.partitionBy("city").orderBy("date").rowsBetween(-29, 0)

# Rolling standard deviation for each pollutant
df_sliding_stddev = df \
    .withColumn("stddev_NO2", stddev("NO2_AQI").over(window_spec)) \
    .withColumn("stddev_O3", stddev("O3_AQI").over(window_spec)) \
    .withColumn("stddev_SO2", stddev("SO2_AQI").over(window_spec)) \
    .withColumn("stddev_CO", stddev("CO_AQI").over(window_spec)) \
    .filter(
    col("stddev_NO2").isNotNull() |
    col("stddev_O3").isNotNull() |
    col("stddev_SO2").isNotNull() |
    col("stddev_CO").isNotNull()
)

# df_sliding_stddev.filter(col("city") == "Roosevelt").orderBy("date").select(
#     "date", "NO2_AQI", "stddev_NO2",
#     "O3_AQI", "stddev_O3",
#     "SO2_AQI", "stddev_SO2",
#     "CO_AQI", "stddev_CO"
# ).show()

# Rank cities based on the rolling standard deviation of each pollutant
city_stddev_avg = df_sliding_stddev.groupBy("city").agg(
    avg(when(~isnan(col("stddev_NO2")) & col("stddev_NO2").isNotNull(), col("stddev_NO2"))).alias("avg_stddev_NO2"),
    avg(when(~isnan(col("stddev_O3")) & col("stddev_O3").isNotNull(), col("stddev_O3"))).alias("avg_stddev_O3"),
    avg(when(~isnan(col("stddev_SO2")) & col("stddev_SO2").isNotNull(), col("stddev_SO2"))).alias("avg_stddev_SO2"),
    avg(when(~isnan(col("stddev_CO")) & col("stddev_CO").isNotNull(), col("stddev_CO"))).alias("avg_stddev_CO")
)

result = city_stddev_avg.withColumn("rank_NO2", row_number().over(Window.orderBy(col("avg_stddev_NO2").desc()))) \
    .withColumn("rank_O3", row_number().over(Window.orderBy(col("avg_stddev_O3").desc()))) \
    .withColumn("rank_SO2", row_number().over(Window.orderBy(col("avg_stddev_SO2").desc()))) \
    .withColumn("rank_CO", row_number().over(Window.orderBy(col("avg_stddev_CO").desc())))

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