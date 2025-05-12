from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    ArrayType, TimestampType
)

# raw JSON schema 
schema = StructType([
    StructField("status", StringType()),
    StructField("data", StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("location", StructType([
            StructField("type", StringType()),
            StructField("coordinates", ArrayType(DoubleType()))
        ])),
        StructField("current", StructType([
            StructField("pollution", StructType([
                StructField("ts", TimestampType()),
                StructField("aqius", IntegerType()),
                StructField("mainus", StringType()),
                StructField("aqicn", IntegerType()),
                StructField("maincn", StringType())
            ])),
            StructField("weather", StructType([
                StructField("ts", TimestampType()),
                StructField("tp", IntegerType()),
                StructField("pr", IntegerType()),
                StructField("hu", IntegerType()),
                StructField("ws", DoubleType()),
                StructField("wd", IntegerType()),
                StructField("ic", StringType())
            ]))
        ]))
    ]))
])

spark = (
    SparkSession.builder
      .appName("AIRQ SCLEANER")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
)

raw = (
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "airq")
      .option("startingOffsets", "earliest")
      .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) as json_str")
       .select(from_json(col("json_str"), schema).alias("json"))
       .select(
           col("json.data.state").alias("state"),           
           col("json.data.city").alias("city"),
           col("json.data.current.pollution.ts").alias("timestamp"),
           col("json.data.current.pollution.aqius").alias("aqi"),
           col("json.data.current.pollution.mainus").alias("pollutant"), # PM1O AND PM2.5
           col("json.data.current.weather.tp").alias("temperature"), # °C
           col("json.data.current.weather.pr").alias("air_pressure"), # hPa
           col("json.data.current.weather.hu").alias("humidity"), # %
           col("json.data.current.weather.ws").alias("wind_speed"), # m/s
           col("json.data.current.weather.wd").alias("wind_direction") # °       
        )
)

output = parsed.selectExpr("CAST(NULL AS STRING) AS key") \
                  .withColumn("value", to_json(struct(
                      "city", "state", "timestamp", "aqi", "pollutant",
                      "temperature", "air_pressure", "humidity",
                      "wind_speed", "wind_direction"
                  )))

query = (
    output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("topic", "cleaned_airq")
        .start()
)

query.awaitTermination()