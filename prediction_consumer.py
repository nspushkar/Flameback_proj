from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, when
from pyspark.sql.types import *
from pyspark.sql.functions import udf

import joblib

# --- Configuration ---
KAFKA_TOPIC = "window-features-ticks"  # Updated to match current feature factory output
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
MODEL_PATH = "xgboost_stateless_model.joblib"

# --- 1. Load Model and Define Prediction UDF ---
# Load the trained model
model = joblib.load(MODEL_PATH)

# Define the schema of the incoming JSON data from your feature factory
features_schema = StructType([
    # Updated schema to match current feature factory output
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

# Create a UDF to apply the model for prediction
def predict_udf(features):
    # The UDF receives a struct of features
    if features is None:
        return None
    # Using available features for prediction - adjust based on your model requirements
    # For now, using price_log, price_volatility_estimate, and current_price as features
    model_input = [[features.price_log, features.price_volatility_estimate, features.current_price]]
    prediction = model.predict(model_input)
    return int(prediction[0])

# Register the UDF
spark_predict_udf = udf(predict_udf, IntegerType())


# --- 2. Main Spark Application ---
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PredictionConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read from the ENRICHED features topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse the incoming JSON from the feature factory
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), features_schema).alias("data")).select("data.*")

    # --- 3. Generate Predictions ---
    
    # Prediction 1: Simple Price Movement Rule
    # A simple rule: if price is above its bucket, predict "UP", else "DOWN"
    df_with_predictions = parsed_df.withColumn(
        "price_movement_prediction",
        when(col("current_price") > col("price_bucket_50"), "UP").otherwise("DOWN")
    )
    
    # Prediction 2: XGBoost Model
    # We pass a struct of the required feature columns to our prediction UDF
    required_features = struct("price_log", "price_volatility_estimate", "current_price")
    df_with_predictions = df_with_predictions.withColumn(
        "xgboost_prediction_code",
        spark_predict_udf(required_features)
    )
    
    # Convert the XGBoost code (1/0) to a readable label
    df_with_predictions = df_with_predictions.withColumn(
        "xgboost_prediction",
        when(col("xgboost_prediction_code") == 1, "UP").otherwise("DOWN")
    )
    
    # --- 4. Display Final Output ---
    final_output = df_with_predictions.select(
        "date",
        "symbol",
        "current_price",
        "price_movement_prediction",
        "xgboost_prediction",
        "price_category",
        "symbol_type",
        "market_session"
    )

    query = final_output.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("--- REAL-TIME PREDICTION ENGINE RUNNING ---")
    print("Reading from topic: 'window-features-ticks'")
    print("Using features: price_log, price_volatility_estimate, current_price")
    query.awaitTermination()