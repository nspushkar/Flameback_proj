from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, struct
from pyspark.sql.window import Window
import json
import math

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FuturesFeatureExtractor") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka configuration
KAFKA_TOPIC = "futures-ticks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Define JSON schema
futures_schema = StructType([
    StructField("date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

print("Starting Time-Window-Based Feature Extraction...")

# Read from Kafka
raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), futures_schema).alias("data")
).select(
    col("data.symbol").alias("symbol"),
    col("data.price").alias("price"),
    col("data.date").alias("date"),
    col("data.timestamp").alias("original_timestamp")
).filter(
    col("price").isNotNull() & col("symbol").isNotNull()
)

# Add processing time and convert to timestamp
enriched_df = parsed_df \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("event_time", to_timestamp(col("original_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("event_time_seconds", unix_timestamp(col("event_time")))

# Calculate features using only basic operations (streaming compatible)
features_df = enriched_df \
    .withColumn("price_squared", col("price") * col("price")) \
    .withColumn("price_sqrt", sqrt(col("price"))) \
    .withColumn("price_log", log(col("price"))) \
    .withColumn("price_reciprocal", 1.0 / col("price")) \
    .withColumn("price_percentage", col("price") * 100.0) \
    .withColumn("price_rounded", round(col("price"), 2)) \
    .withColumn("price_ceiling", ceil(col("price"))) \
    .withColumn("price_floor", floor(col("price"))) \
    .withColumn("price_abs", abs(col("price"))) \
    .withColumn("price_sign", signum(col("price"))) \
    .withColumn("symbol_length", length(col("symbol"))) \
    .withColumn("symbol_upper", upper(col("symbol"))) \
    .withColumn("symbol_lower", lower(col("symbol"))) \
    .withColumn("date_year", year(col("event_time"))) \
    .withColumn("date_month", month(col("event_time"))) \
    .withColumn("date_day", dayofmonth(col("event_time"))) \
    .withColumn("date_hour", hour(col("event_time"))) \
    .withColumn("date_minute", minute(col("event_time"))) \
    .withColumn("date_second", second(col("event_time"))) \
    .withColumn("day_of_week", dayofweek(col("event_time"))) \
    .withColumn("day_of_year", dayofyear(col("event_time"))) \
    .withColumn("week_of_year", weekofyear(col("event_time"))) \
    .withColumn("quarter", quarter(col("event_time"))) \
    .withColumn("unix_timestamp", unix_timestamp(col("event_time"))) \
    .withColumn("price_bucket_10", floor(col("price") / 10.0) * 10) \
    .withColumn("price_bucket_50", floor(col("price") / 50.0) * 50) \
    .withColumn("price_bucket_100", floor(col("price") / 100.0) * 100) \
    .withColumn("price_category", 
        when(col("price") < 50, "low")
        .when(col("price") < 200, "medium")
        .when(col("price") < 1000, "high")
        .otherwise("very_high")
    ) \
    .withColumn("symbol_type", 
        when(col("symbol").contains("Index"), "index")
        .when(col("symbol").contains("Comdty"), "commodity")
        .otherwise("other")
    ) \
    .withColumn("price_volatility_estimate", 
        when(col("price") < 100, 0.05)
        .when(col("price") < 500, 0.03)
        .when(col("price") < 2000, 0.02)
        .otherwise(0.01)
    ) \
    .withColumn("market_session", 
        when(col("date_hour") < 12, "morning")
        .when(col("date_hour") < 17, "afternoon")
        .otherwise("evening")
    )

# Select final features
final_features = features_df.select(
        col("symbol"),
        col("price").alias("current_price"),
        col("date"),
        col("processing_time"),
    col("price_squared"),
    col("price_sqrt"),
    col("price_log"),
    col("price_reciprocal"),
    col("price_percentage"),
    col("price_rounded"),
    col("price_ceiling"),
    col("price_floor"),
    col("price_abs"),
    col("price_sign"),
    col("symbol_length"),
    col("symbol_upper"),
    col("symbol_lower"),
    col("date_year"),
    col("date_month"),
    col("date_day"),
    col("date_hour"),
    col("date_minute"),
    col("date_second"),
    col("day_of_week"),
    col("day_of_year"),
    col("week_of_year"),
    col("quarter"),
    col("unix_timestamp"),
    col("price_bucket_10"),
    col("price_bucket_50"),
    col("price_bucket_100"),
    col("price_category"),
    col("symbol_type"),
    col("price_volatility_estimate"),
    col("market_session")
    )

# Output the results
kafka_output_df = final_features.select(to_json(struct("*")).alias("value"))

# Write the stream to the 'window-features-ticks' topic
query = kafka_output_df \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", "window-features-ticks") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/window_factory") \
    .trigger(processingTime='2 seconds') \
    .start()

print("="*50)
print("Streaming BASIC FEATURES to Kafka topic: 'window-features-ticks'")
print("Features calculated using basic operations (streaming compatible):")
print("1. Price transformations (squared, sqrt, log, reciprocal, etc.)")
print("2. Symbol features (length, case, type)")
print("3. Date/time features (year, month, day, hour, etc.)")
print("4. Price buckets and categories")
print("5. Market session indicators")
print("6. Volatility estimates")
print("7. All features compatible with PySpark Structured Streaming")
print("="*50)

query.awaitTermination()