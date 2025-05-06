import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import month, year, when, col, avg, row_number, desc

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b10"

# Spark init
spark = SparkSession.builder \
    .appName("Identifying seasonal patterns in monthly pollutant concentrations") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

# Extract month and year from the date column
df_seasonal = df.withColumn("month", month("date")) \
    .withColumn("year", year("date"))

# Define seasons based on months
df_seasonal = df_seasonal.withColumn(
    "season",
    when(col("month").between(3, 5), "Spring")
     .when(col("month").between(6, 8), "Summer")
     .when(col("month").between(9, 11), "Fall")
     .otherwise("Winter")
)

# Calculate the average AQI for each pollutant by year, month, and season
df_seasonal_avg = df_seasonal.groupBy("year", "season") \
    .agg(
        avg("NO2_AQI").alias("avg_NO2_AQI"),
        avg("O3_AQI").alias("avg_O3_AQI"),
        avg("SO2_AQI").alias("avg_SO2_AQI"),
        avg("CO_AQI").alias("avg_CO_AQI")
    )

# Rank by season
season_window_spec = Window.partitionBy("season").orderBy(desc("avg_NO2_AQI"))
result= df_seasonal_avg \
    .withColumn("rank_NO2", row_number().over(season_window_spec)) \
    .withColumn("rank_O3", row_number().over(Window.partitionBy("season").orderBy(desc("avg_O3_AQI")))) \
    .withColumn("rank_SO2", row_number().over(Window.partitionBy("season").orderBy(desc("avg_SO2_AQI")))) \
    .withColumn("rank_CO", row_number().over(Window.partitionBy("season").orderBy(desc("avg_CO_AQI"))))


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