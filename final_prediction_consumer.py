from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, when, udf
from pyspark.sql.types import *
import joblib
import pandas as pd
import sqlite3

# --- Configuration & Model Loading (Same as before) ---
KAFKA_TOPIC = "estimated-features-ticks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "predictions.db"
TABLE_NAME = "predictions"

XGBOOST_MODEL_PATH = "xgboost_model.joblib"
ADABOOST_MODEL_PATH = "adaboost_model.joblib"
RANDOMFOREST_MODEL_PATH = "randomforest_model.joblib"
LOGREG_MODEL_PATH = "logisticregression_model.joblib"

xgboost_model = joblib.load(XGBOOST_MODEL_PATH)
adaboost_model = joblib.load(ADABOOST_MODEL_PATH)
randomforest_model = joblib.load(RANDOMFOREST_MODEL_PATH)
logreg_model = joblib.load(LOGREG_MODEL_PATH)

features_schema = StructType([
    StructField("symbol", StringType()), StructField("current_price", DoubleType()),
    StructField("date", StringType()), StructField("processing_time", StringType()),
    StructField("simple_return", DoubleType()), StructField("log_return", DoubleType()),
    StructField("sma_5", DoubleType()), StructField("sma_10", DoubleType()),
    StructField("sma_20", DoubleType()), StructField("ema_12", DoubleType()),
    StructField("ema_26", DoubleType()), StructField("macd_line", DoubleType()),
    StructField("macd_signal", DoubleType()), StructField("macd_histogram", DoubleType()),
    StructField("volatility_10", DoubleType()), StructField("volatility_20", DoubleType()),
    StructField("bb_upper", DoubleType()), StructField("bb_lower", DoubleType()),
    StructField("bb_position", DoubleType()), StructField("rsi", DoubleType()),
    StructField("lag_1", DoubleType()), StructField("lag_2", DoubleType()),
    StructField("lag_5", DoubleType()), StructField("price_percentile", StringType())
])

def create_prediction_udf(model, feature_order):
    def predict(features):
        if features is None: return None
        model_input_list = [features[feature] for feature in feature_order]
        model_input = [model_input_list]
        prediction = model.predict(model_input)
        return int(prediction[0])
    return udf(predict, IntegerType())

feature_order_for_models = ["simple_return", "lag_1", "current_price"]
xgboost_predict_udf = create_prediction_udf(xgboost_model, feature_order_for_models)
adaboost_predict_udf = create_prediction_udf(adaboost_model, feature_order_for_models)
randomforest_predict_udf = create_prediction_udf(randomforest_model, feature_order_for_models)
logreg_predict_udf = create_prediction_udf(logreg_model, feature_order_for_models)

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
    
    df_with_predictions = parsed_df.withColumn("Momentum", when(col("simple_return") > 0, "UP").otherwise("DOWN"))
    required_features = struct(*feature_order_for_models)
    df_with_predictions = df_with_predictions.withColumn("XGBoost", when(xgboost_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
    df_with_predictions = df_with_predictions.withColumn("AdaBoost", when(adaboost_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
    df_with_predictions = df_with_predictions.withColumn("RandomForest", when(randomforest_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
    df_with_predictions = df_with_predictions.withColumn("LogisticReg", when(logreg_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
    
    # --- NEW: Select ALL the columns we need for the dashboard ---
    final_output = df_with_predictions.select(
    "date", "symbol", "current_price", "simple_return",
    "sma_10", "sma_20", "volatility_10", "rsi", "bb_upper", "bb_lower",
    "Momentum", "XGBoost", "AdaBoost", "RandomForest", "LogisticReg"
    )

    query = final_output.writeStream.foreachBatch(write_to_sqlite).outputMode("append").start()
    print("--- REAL-TIME PREDICTION ENGINE WRITING TO DATABASE ---")
    query.awaitTermination()























# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, struct, when, udf
# from pyspark.sql.types import *
# import joblib
# import pandas as pd

# # --- Configuration ---
# KAFKA_TOPIC = "estimated-features-ticks"
# KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# # --- NEW: Paths to all our trained models ---
# XGBOOST_MODEL_PATH = "xgboost_model.joblib"
# ADABOOST_MODEL_PATH = "adaboost_model.joblib"
# RANDOMFOREST_MODEL_PATH = "randomforest_model.joblib"
# LOGREG_MODEL_PATH = "logisticregression_model.joblib"

# # --- 1. Load All Models ---
# print("Loading all trained models...")
# xgboost_model = joblib.load(XGBOOST_MODEL_PATH)
# adaboost_model = joblib.load(ADABOOST_MODEL_PATH)
# randomforest_model = joblib.load(RANDOMFOREST_MODEL_PATH)
# logreg_model = joblib.load(LOGREG_MODEL_PATH)
# print("Models loaded successfully.")

# # Define the schema of the incoming JSON data from your feature factory
# # IMPORTANT: This must be complete and match your factory's output
# features_schema = StructType([
#     StructField("symbol", StringType()),
#     StructField("current_price", DoubleType()),
#     StructField("date", StringType()),
#     StructField("processing_time", StringType()),
#     StructField("simple_return", DoubleType()),
#     StructField("log_return", DoubleType()),
#     StructField("sma_5", DoubleType()),
#     StructField("sma_10", DoubleType()),
#     StructField("sma_20", DoubleType()),
#     StructField("ema_12", DoubleType()),
#     StructField("ema_26", DoubleType()),
#     StructField("macd_line", DoubleType()),
#     StructField("macd_signal", DoubleType()),
#     StructField("macd_histogram", DoubleType()),
#     StructField("volatility_10", DoubleType()),
#     StructField("volatility_20", DoubleType()),
#     StructField("bb_upper", DoubleType()),
#     StructField("bb_lower", DoubleType()),
#     StructField("bb_position", DoubleType()),
#     StructField("rsi", DoubleType()),
#     StructField("lag_1", DoubleType()),
#     StructField("lag_2", DoubleType()),
#     StructField("lag_5", DoubleType()),
#     StructField("price_percentile", StringType())
# ])

# # --- NEW: Create a separate UDF for each model ---
# def create_prediction_udf(model, feature_order):
#     """A helper function to create a UDF for a given scikit-learn or XGBoost model."""
#     def predict(features):
#         if features is None:
#             return None
#         # Create a list of feature values in the correct order
#         model_input_list = [features[feature] for feature in feature_order]
#         # Models expect a 2D array, so we wrap our list in another list
#         model_input = [model_input_list]
#         prediction = model.predict(model_input)
#         return int(prediction[0])
#     return udf(predict, IntegerType())

# # Define the exact order of features used during training
# feature_order_for_models = ["simple_return", "lag_1", "current_price"]

# # Register a UDF for each model
# xgboost_predict_udf = create_prediction_udf(xgboost_model, feature_order_for_models)
# adaboost_predict_udf = create_prediction_udf(adaboost_model, feature_order_for_models)
# randomforest_predict_udf = create_prediction_udf(randomforest_model, feature_order_for_models)
# logreg_predict_udf = create_prediction_udf(logreg_model, feature_order_for_models)

# # --- 2. Main Spark Application ---
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("MultiModelPredictionConsumer").getOrCreate()
#     spark.sparkContext.setLogLevel("WARN")

#     kafka_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#         .option("subscribe", KAFKA_TOPIC) \
#         .load()

#     parsed_df = kafka_df.select(from_json(col("value").cast("string"), features_schema).alias("data")).select("data.*")

#     # --- 3. Generate Predictions from All Sources ---
    
#     # Prediction 1: Traditional Momentum Rule
#     df_with_predictions = parsed_df.withColumn(
#         "Momentum",
#         when(col("simple_return") > 0, "UP").otherwise("DOWN")
#     )
    
#     # Create a struct of the required features to pass to the UDFs
#     required_features = struct(*feature_order_for_models)

#     # --- NEW: Add predictions for all ML models ---
#     df_with_predictions = df_with_predictions.withColumn("XGBoost", when(xgboost_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
#     df_with_predictions = df_with_predictions.withColumn("AdaBoost", when(adaboost_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
#     df_with_predictions = df_with_predictions.withColumn("RandomForest", when(randomforest_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
#     df_with_predictions = df_with_predictions.withColumn("LogisticReg", when(logreg_predict_udf(required_features) == 1, "UP").otherwise("DOWN"))
    
#     # --- 4. Display Final Output ---
#     # Select all the predictions for a clean side-by-side comparison
#     final_output = df_with_predictions.select(
#         "date",
#         "symbol",
#         "current_price",
#         "Momentum",
#         "XGBoost",
#         "AdaBoost",
#         "RandomForest",
#         "LogisticReg"
#     )

#     query = final_output.writeStream \
#         .outputMode("append") \
#         .format("console") \
#         .option("truncate", "false") \
#         .start()

#     print("--- REAL-TIME MULTI-MODEL PREDICTION ENGINE RUNNING ---")
#     query.awaitTermination()