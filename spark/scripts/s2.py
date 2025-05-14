from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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
    .appName("Make high temperature alert") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/airq.s2") \
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

high_temp_df = parsed_df.filter(col("temperature") > 30)

query = high_temp_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/home/checkpoints/high_temp/s2") \
    .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017/airq.s2") \
    .outputMode("append") \
    .start()

query.awaitTermination()