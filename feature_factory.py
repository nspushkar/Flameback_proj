from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.streaming.state import GroupStateTimeout

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StatefulFeatureFactory") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka setup
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_RAW_TOPIC = "futures-ticks"

# Schema
kafka_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("date", StringType(), True),
])

# State schema and output schema
state_schema = StructType([StructField("last_price", DoubleType(), True)])
output_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

# Stateful function using Pandas
def calculate_features(key, pdf_iter, state):
    import pandas as pd
    for pdf in pdf_iter:
        pdf = pdf.sort_values("timestamp")
        last_price = state.get("last_price") if state.exists else None
        price_changes = []
        for row in pdf.itertuples():
            change = 0.0 if last_price is None else row.price - last_price
            price_changes.append((row.symbol, change, row.timestamp))
            last_price = row.price
        state.update({"last_price": last_price})
        yield pd.DataFrame(price_changes, columns=["symbol", "price_change", "timestamp"])

# Read from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_RAW_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), kafka_schema).alias("data")) \
    .select("data.*")

df_with_ts = parsed_df.withColumn("timestamp", col("date").cast(TimestampType()))

# Apply applyInPandasWithState (Spark 3.2+)
features_stream = df_with_ts.groupBy("symbol").applyInPandasWithState(
    func=calculate_features,
    outputStructType=output_schema,
    stateStructType=state_schema,
    outputMode="append",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
)

# Output to console
query = features_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
