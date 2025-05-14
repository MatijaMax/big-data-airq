from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Define the schema for the JSON data
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

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("AirQualityStreamProcessor") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/airq_db") \
    .getOrCreate()

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cleaned_airq") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

max_temp_df = parsed_df \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(
        window(col("timestamp"), "15 minutes"),
        col("state")
    ) \
    .max("temperature") \
    .withColumnRenamed("max(temperature)", "max_temperature")

# Write to MongoDB
query = max_temp_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/checkpoints/max_temp") \
    .option("collection", "max_temp_per_state") \
    .outputMode("complete") \
    .start()

query.awaitTermination()