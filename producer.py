import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime

# --- Configuration ---
KAFKA_TOPIC = 'futures-ticks'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
DATA_SOURCE_FILE = 'timeseries_data_long.csv'
SIMULATION_SPEED_SECONDS = 0.01 # Time to wait between sending messages

# --- Initialize Kafka Producer ---
# The value_serializer encodes our dictionary as a JSON string, then into bytes
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Successfully connected to Kafka producer.")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit()

# --- Load the prepared data ---
try:
    df = pd.read_csv(DATA_SOURCE_FILE)
    print(f"Successfully loaded data from '{DATA_SOURCE_FILE}'. Starting simulation...")
except FileNotFoundError:
    print(f"Error: Data source file '{DATA_SOURCE_FILE}' not found.")
    exit()

# --- Loop through data and send to Kafka ---
# We iterate through the DataFrame rows and send each one as a message.
for index, row in df.iterrows():
    # Create a timestamp from the date (assuming 9:30 AM for market open)
    date_str = row['date']
    timestamp_str = f"{date_str} 09:30:00"
    
    # Construct the message from the row data
    message = {
        'date': row['date'],
        'symbol': row['symbol'],
        'price': row['price'],
        'timestamp': timestamp_str  # Add the timestamp field that feature factory expects
    }

    try:
        # Send the message to the specified Kafka topic
        producer.send(KAFKA_TOPIC, value=message)

        # Print to console to show progress (less verbose)
        if index % 1000 == 0:
            print(f"Sent {index} messages...")

        # Wait for a short period to simulate a real-time stream
        time.sleep(SIMULATION_SPEED_SECONDS)

    except Exception as e:
        print(f"Error sending message: {e}")
        time.sleep(1) # Wait a bit before retrying or exiting

# --- Clean up ---
# Ensure all buffered messages are sent before the script exits
producer.flush()
print("All data has been sent to Kafka.")
producer.close()