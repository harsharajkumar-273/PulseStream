import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, min, max, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, MapType

# Kafka & Delta Configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:29092")
KAFKA_TOPIC = "metrics.raw"
DELTA_PATH = "/opt/spark/data/aggregated_metrics"
CHECKPOINT_PATH = "/opt/spark/data/aggregated_metrics_checkpoint"

# 1. Initialize Spark Session with Delta Lake extension jars configured
spark = SparkSession.builder \
    .appName("PulseStreamSparkProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🔥 Spark Session initialized with Kafka & Delta support")

# 2. Define the schema of the incoming JSON events from Kafka
event_schema = StructType([
    StructField("id", StringType(), True),
    StructField("deviceId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

# 3. Read stream from Redpanda
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse key and value, convert timestamp (unix milliseconds) to Spark Timestamp
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), event_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp") / 1000.0))

# 5. Apply Watermarking and Sliding Window Aggregations
# Watermark: 10 minutes (discards data arriving > 10m late)
# Window: 5 minutes sliding every 1 minute
aggregated_df = parsed_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("eventType")
    ) \
    .agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        count("id").alias("event_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("eventType"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("event_count")
    )

# 6. Write stream to Delta Lake Table
print(f"📁 Streaming aggregations will be written to Delta table at: {DELTA_PATH}")

query = aggregated_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(DELTA_PATH)

# Block thread until termination
query.awaitTermination()
