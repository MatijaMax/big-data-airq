import os
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql import Row

# Mongo init
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "airq"
MONGO_COLLECTION = "b9"

# Spark init
spark = SparkSession.builder \
    .appName("Ranking countries based on the linear regression trend for NO2 AQI") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/clean/"

# Read data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

# Convert date to numeric 
df = df.withColumn("date_numeric", monotonically_increasing_id())

# Prepare data for regression
results = []
for state in df.select("state").distinct().collect():
    state_name = state["state"]
    state_df = df.filter(df.state == state_name).select("date_numeric", "NO2_AQI")

    # Some countries may not have enough data
    if state_df.count() < 2:
        continue

    # Assemble features
    assembler = VectorAssembler(inputCols=["date_numeric"], outputCol="features")
    assembled = assembler.transform(state_df).select("features", col("NO2_AQI").alias("label"))

    # Fit linear regression model
    lr = LinearRegression()
    model = lr.fit(assembled)

    # Extract slope (coefficient)
    slope = float(model.coefficients[0])
    results.append(Row(country=state_name, NO2_slope=slope))

# Create DataFrame from results
result = spark.createDataFrame(results)

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