# Flameback Capital: Real-Time Financial Signal Terminal

This project implements a high-throughput, real-time streaming data pipeline that ingests simulated financial tick data, enriches it with calculated features, applies multiple machine learning models for price prediction, and visualizes the results on a dynamic, Bloomberg-inspired dashboard.


*(Suggestion: Run your dashboard, take a screenshot, upload it to your GitHub issue or a new "img" folder, and replace the link above to showcase your final product!)*

## Key Features

*   **Real-Time Data Ingestion:** A Python producer simulates a live feed of financial tick data into an Apache Kafka topic.
*   **Distributed Stream Processing:** Apache Spark (PySpark) Structured Streaming is used for scalable, fault-tolerant data processing.
*   **Multi-Stage Pipeline Architecture:** The system is compartmentalized into two distinct Spark jobs: a **Feature Factory** and a **Prediction Engine**, communicating via a second Kafka topic for modularity and resilience.
*   **Live Feature Engineering:** The first Spark job consumes raw ticks and calculates a rich set of features (returns, SMAs, volatility, RSI, MACD, etc.) on the fly.
*   **Multi-Model ML Inference:** The second Spark job consumes the enriched data, loads multiple pre-trained models (XGBoost, AdaBoost, RandomForest, etc.), and generates real-time UP/DOWN predictions.
*   **Interactive Dashboard:** A Streamlit application reads the final predictions from a SQLite database and presents them in a sleek, auto-refreshing interface with technical charts and live signal indicators.

## Project Architecture

The pipeline follows a multi-stage, publish-subscribe model, ensuring loose coupling between components.

```
[producer.py] -> (Kafka Topic: futures-ticks) -> [stateless_feature_factory.py] -> (Kafka Topic: estimated-features-ticks) -> [prediction_engine.py] -> (SQLite DB) -> [dashboard.py]
```
1.  **Raw Data Producer:** Simulates tick data into the `futures-ticks` topic.
2.  **Feature Factory (Spark Job 1):** Consumes from `futures-ticks`, calculates features, and produces to the `estimated-features-ticks` topic.
3.  **Prediction Engine (Spark Job 2):** Consumes from `estimated-features-ticks`, applies ML models, and writes final predictions to the `predictions.db` file.
4.  **Dashboard:** Reads from `predictions.db` and visualizes the data.

## Technology Stack

*   **Data Streaming:** Apache Kafka
*   **Data Processing:** Apache Spark 3.5.3 (PySpark, Structured Streaming)
*   **Machine Learning:** Scikit-learn, XGBoost
*   **Dashboard:** Streamlit, Plotly
*   **Database:** SQLite3
*   **Core Language & Libraries:** Python, Pandas, NumPy

## Setup Instructions

Follow these steps to set up the project environment on macOS.

#### 1. Prerequisites
*   [Homebrew](https://brew.sh/) package manager.
*   Java JDK 11+ (`brew install openjdk`).
*   Python 3.9+.

#### 2. Install Core Services
Install Apache Kafka and Apache Spark using Homebrew.
```bash
brew install kafka apache-spark
```

#### 3. Clone the Repository
```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```

#### 4. Install Python Dependencies
It is highly recommended to use a Python virtual environment.
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### 5. Train the Machine Learning Models
Before running the live pipeline, you must first train the ML models on the historical data. This script will generate the necessary `.joblib` model files.
```bash
python train_models_unified.py
```

## How to Run the Full Pipeline

Start each component in a **separate terminal window**, in the following order.

#### 1. Start Kafka & Zookeeper Services
If they are not already running, start them as background services.
```bash
brew services start kafka
```

#### 2. Terminal 1: Start the Data Producer
This script reads from the source CSVs and starts the live data feed.
```bash
python producer.py
```

#### 3. Terminal 2: Start the Feature Factory
This Spark job consumes raw ticks and produces enriched data with features. *Ensure the Spark version in the package matches your installation.*
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 stateless_feature_factory.py
```

#### 4. Terminal 3: Start the Prediction Engine
This Spark job consumes the enriched features, runs the models, and writes to the database.
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,pyarrow:10.0.1 prediction_consumer.py
```
*(Wait for this job to initialize and start writing batches to the database.)*

#### 5. Terminal 4: Launch the Streamlit Dashboard
This launches the web application.
```bash
streamlit run dashboard.py
```
A browser window will open with the real-time terminal.