from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, when, udf
from pyspark.sql.types import *
import pandas as pd
import sqlite3

# --- Configuration ---
KAFKA_TOPIC = "window-features-ticks"  # Updated to match current feature factory
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "predictions.db"
TABLE_NAME = "predictions"

# Updated schema to match current feature factory output
features_schema = StructType([
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("date", StringType()),
    StructField("processing_time", StringType()),
    StructField("price_squared", DoubleType()),
    StructField("price_sqrt", DoubleType()),
    StructField("price_log", DoubleType()),
    StructField("price_reciprocal", DoubleType()),
    StructField("price_percentage", DoubleType()),
    StructField("price_rounded", DoubleType()),
    StructField("price_ceiling", DoubleType()),
    StructField("price_floor", DoubleType()),
    StructField("price_abs", DoubleType()),
    StructField("price_sign", DoubleType()),
    StructField("symbol_length", IntegerType()),
    StructField("symbol_upper", StringType()),
    StructField("symbol_lower", StringType()),
    StructField("date_year", IntegerType()),
    StructField("date_month", IntegerType()),
    StructField("date_day", IntegerType()),
    StructField("date_hour", IntegerType()),
    StructField("date_minute", IntegerType()),
    StructField("date_second", IntegerType()),
    StructField("day_of_week", IntegerType()),
    StructField("day_of_year", IntegerType()),
    StructField("week_of_year", IntegerType()),
    StructField("quarter", IntegerType()),
    StructField("unix_timestamp", LongType()),
    StructField("price_bucket_10", DoubleType()),
    StructField("price_bucket_50", DoubleType()),
    StructField("price_bucket_100", DoubleType()),
    StructField("price_category", StringType()),
    StructField("symbol_type", StringType()),
    StructField("price_volatility_estimate", DoubleType()),
    StructField("market_session", StringType())
])

def write_to_sqlite(batch_df, batch_id):
    pandas_df = batch_df.toPandas()
    if pandas_df.empty: return
    try:
        with sqlite3.connect(DB_PATH) as conn:
            pandas_df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
        print(f"--- Wrote batch {batch_id} with {len(pandas_df)} rows to SQLite ---")
    except Exception as e:
        print(f"Error writing to SQLite: {e}")

# --- Main Spark Application ---
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PredictionWriter").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("subscribe", KAFKA_TOPIC).load()
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), features_schema).alias("data")).select("data.*")
    
    # --- Simple Rule-Based Predictions ---
    # Prediction 1: Price Movement based on price vs bucket
    df_with_predictions = parsed_df.withColumn(
        "PriceMovement", 
        when(col("current_price") > col("price_bucket_50"), "UP").otherwise("DOWN")
    )
    
    # Prediction 2: XGBoost-like prediction based on price_log and volatility
    df_with_predictions = df_with_predictions.withColumn(
        "XGBoost", 
        when((col("price_log") > 5.0) & (col("price_volatility_estimate") < 0.03), "UP").otherwise("DOWN")
    )
    
    # Prediction 3: AdaBoost-like prediction based on price category
    df_with_predictions = df_with_predictions.withColumn(
        "AdaBoost", 
        when(col("price_category").isin(["high", "very_high"]), "UP").otherwise("DOWN")
    )
    
    # Prediction 4: RandomForest-like prediction based on symbol type and market session
    df_with_predictions = df_with_predictions.withColumn(
        "RandomForest", 
        when((col("symbol_type") == "index") & (col("market_session") == "morning"), "UP").otherwise("DOWN")
    )
    
    # Prediction 5: Logistic Regression-like prediction based on day of week
    df_with_predictions = df_with_predictions.withColumn(
        "LogisticReg", 
        when(col("day_of_week").isin([1, 2, 3, 4, 5]), "UP").otherwise("DOWN")  # Weekdays
    )
    
    # --- Select columns for the dashboard ---
    final_output = df_with_predictions.select(
        "date", "symbol", "current_price", "price_log", "price_volatility_estimate",
        "price_category", "symbol_type", "market_session", "day_of_week",
        "price_bucket_50", "price_sqrt", "price_squared",
        "PriceMovement", "XGBoost", "AdaBoost", "RandomForest", "LogisticReg"
    )

    query = final_output.writeStream.foreachBatch(write_to_sqlite).outputMode("append").start()
    print("--- REAL-TIME PREDICTION ENGINE WRITING TO DATABASE ---")
    print("Reading from topic: 'window-features-ticks'")
    print("Using simple rule-based predictions (no ML models)")
    query.awaitTermination()