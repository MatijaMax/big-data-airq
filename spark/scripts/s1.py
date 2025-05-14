import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# JSON schema
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

# Init
spark = SparkSession.builder \
    .appName("Detect sudden AQI spikes") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/airq.s1") \
    .getOrCreate()

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cleaned_airq") \
    .load()

# Parse data
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

aqi_spikes_df = parsed_df \
    .filter(col("aqi") > 50)

# Write to MongoDB
query = aqi_spikes_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/home/checkpoints/aqi_spikes/s1") \
    .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017/airq.s1") \
    .outputMode("append") \
    .start()

query.awaitTermination()