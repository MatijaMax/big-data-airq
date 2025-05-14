from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("aqi", IntegerType()),
    StructField("pollutant", StringType()),
    StructField("temperature", IntegerType()),
    StructField("air_pressure", IntegerType()),
    StructField("humidity", IntegerType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_direction", IntegerType())
])

spark = SparkSession.builder \
    .appName("Get AQI windowed average") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/airq.s5") \
    .getOrCreate()

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cleaned_airq") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

windowed_avg_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        #window(col("timestamp"), "30 seconds"),
        window(col("timestamp"), "10 minutes"),
        col("state")
    ).agg(avg("aqi").alias("avg_aqi"))

query = windowed_avg_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/home/checkpoints/windowing_avg/s5") \
    .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017/airq.s5") \
    .outputMode("append") \
    .start()

query.awaitTermination()