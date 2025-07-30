# Flameback Capital: Real-Time ML Trading Terminal

A sophisticated real-time financial data pipeline that generates **actual technical indicators** and uses **real machine learning models** for price prediction. This system processes live market data, calculates professional-grade technical analysis indicators, applies multiple ML algorithms, and visualizes everything on a Bloomberg-inspired trading terminal.

![Trading Terminal](https://via.placeholder.com/800x400/000000/FFFFFF?text=Flameback+Trading+Terminal)

## 🚀 Key Features

*   **Real Technical Indicators**: SMA, EMA, RSI, MACD, Bollinger Bands, Volatility, Momentum - calculated using proper window functions and historical data
*   **Real Machine Learning Models**: XGBoost, RandomForest, AdaBoost, LogisticRegression trained on actual technical features
*   **Live Feature Engineering**: Pure Python stateful processing that maintains price history for accurate technical calculations
*   **Multi-Model ML Inference**: Real-time predictions from 4 different ML models plus 5 rule-based trading signals
*   **Professional Dashboard**: Bloomberg-style interface with color-coded indicators, live charts, and real-time signal updates
*   **High-Performance Pipeline**: Kafka-based streaming architecture with SQLite persistence

## 🏗️ Project Architecture

The pipeline follows a modern streaming architecture with real technical analysis:

```
[producer.py] → (Kafka: futures-ticks) → [real_feature_factory.py] → (Kafka: real-features-ticks) → [real_ml_consumer.py] → (SQLite DB) → [dashboard.py]
```

1.  **Data Producer**: Simulates live financial tick data into `futures-ticks` topic
2.  **Real Feature Factory**: Pure Python application that maintains price history and calculates actual technical indicators
3.  **ML Prediction Engine**: Consumes real technical features, applies trained ML models, and generates predictions
4.  **Trading Dashboard**: Real-time visualization with professional trading terminal interface

## 🔧 Technical Indicators Generated

### Moving Averages
- **SMA (5, 10, 20)**: Simple Moving Averages
- **EMA (12, 26)**: Exponential Moving Averages

### Momentum Indicators
- **RSI (14)**: Relative Strength Index with proper gain/loss calculations
- **MACD**: Moving Average Convergence Divergence with signal line and histogram
- **Momentum (10)**: Price momentum over 10 periods

### Volatility & Bands
- **Bollinger Bands (20, 2σ)**: Upper, middle, and lower bands
- **Volatility (20)**: Annualized volatility using rolling standard deviation

### Price Analysis
- **Price Change**: Absolute and percentage price changes
- **Lagged Prices**: Historical price references (lag 1, 2, 5)

## 🤖 Machine Learning Models

### Trained Models
- **XGBoost**: Gradient boosting with technical features
- **RandomForest**: Ensemble learning for robust predictions
- **AdaBoost**: Adaptive boosting algorithm
- **LogisticRegression**: Linear model for binary classification

### Rule-Based Signals
- **RSI Rule**: Buy when RSI > 50, Sell when RSI < 50
- **MACD Rule**: Buy when MACD > 0, Sell when MACD < 0
- **Momentum Rule**: Buy when momentum > 0, Sell when momentum < 0
- **Price Change Rule**: Buy when price change > 0, Sell when price change < 0
- **Bollinger Bands Rule**: Buy when oversold, Sell when overbought

## 🛠️ Technology Stack

*   **Data Streaming**: Apache Kafka
*   **Data Processing**: Pure Python with stateful processing
*   **Machine Learning**: Scikit-learn, XGBoost
*   **Dashboard**: Streamlit, Plotly
*   **Database**: SQLite3
*   **Core Libraries**: Pandas, NumPy, Collections (deque)

## 📦 Setup Instructions

### 1. Prerequisites
*   [Homebrew](https://brew.sh/) package manager
*   Java JDK 11+ (`brew install openjdk`)
*   Python 3.9+

### 2. Install Core Services
```bash
brew install kafka
```

### 3. Clone the Repository
```bash
git clone https://github.com/your-username/flameback-trading.git
cd flameback-trading
```

### 4. Install Python Dependencies
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Start Kafka Services
```bash
brew services start kafka
```

## 🚀 How to Run the Full Pipeline

Start each component in **separate terminal windows** in the following order:

### 1. Terminal 1: Start the Data Producer
```bash
python producer.py
```
This simulates live financial data from historical CSV files.

### 2. Terminal 2: Start the Real Feature Factory
```bash
python real_feature_factory.py
```
This calculates actual technical indicators using proper window functions and maintains price history.

### 3. Terminal 3: Start the ML Prediction Engine
```bash
python real_ml_consumer.py
```
This applies real ML models and rule-based strategies to generate predictions.

### 4. Terminal 4: Launch the Trading Dashboard
```bash
streamlit run dashboard.py
```
Access the professional trading terminal at http://localhost:8501

## 📊 Dashboard Features

### Real-Time KPIs
- **Current Price**: Live price updates
- **RSI (14)**: Color-coded (Green: >70, Red: <30, Yellow: 30-70)
- **MACD**: Color-coded (Green: >0, Red: <0)
- **Volatility**: Rolling 20-period volatility

### Technical Charts
- **Price Action**: Live price line with SMA overlays
- **Moving Averages**: SMA (10) and SMA (20) trend lines
- **Bollinger Bands**: Upper and lower volatility bands
- **Interactive**: Zoom, pan, and hover capabilities

### ML Predictions
- **4 ML Models**: XGBoost, RandomForest, AdaBoost, LogisticRegression
- **5 Rule-Based Signals**: RSI, MACD, Momentum, Price Change, Bollinger Bands
- **Visual Signals**: Color-coded arrows (▲ Green: UP, ▼ Red: DOWN, ● Yellow: NEUTRAL)

### Technical Analysis
- **Momentum Indicators**: Real-time momentum calculations
- **Price Changes**: Percentage and absolute changes
- **Moving Averages**: Current SMA values
- **Volatility Metrics**: Rolling volatility measures

## 📈 Sample Output

The system processes **39 unique symbols** with **real-time technical indicators**:

```
📊 ES1 Index: Price=$1270.57 | RSI=67.40 | MACD=-2.7492 | Vol=0.150
   ML: {'XGBoost': 'DOWN', 'RandomForest': 'UP', 'AdaBoost': 'DOWN', 'LogisticReg': 'UP'}
   Rules: {'RSI_Rule': 'UP', 'MACD_Rule': 'DOWN', 'Momentum_Rule': 'UP', 'PriceChange_Rule': 'DOWN', 'BB_Rule': 'NEUTRAL'}
```

## 🔍 Database Schema

The system writes comprehensive data to `predictions.db`:

```sql
CREATE TABLE predictions (
    symbol TEXT,
    current_price REAL,
    timestamp TEXT,
    processing_time TEXT,
    rsi_14 REAL,
    macd_line REAL,
    volatility_20 REAL,
    momentum_10 REAL,
    price_change_pct REAL,
    sma_10 REAL,
    sma_20 REAL,
    bb_upper REAL,
    bb_lower REAL,
    XGBoost TEXT,
    RandomForest TEXT,
    AdaBoost TEXT,
    LogisticReg TEXT,
    RSI_Rule TEXT,
    MACD_Rule TEXT,
    Momentum_Rule TEXT,
    PriceChange_Rule TEXT,
    BB_Rule TEXT
);
```

## 🔄 **Complete System Workflow**

### **Phase 1: Data Ingestion & Production**
```
📊 Historical CSV Data → producer.py → Kafka Topic: futures-ticks
```

**What happens:**
1. **`producer.py`** reads historical financial data from CSV files (`Seas Port Final.csv`, `Tick_Data_SymbolList_Futures.csv`)
2. **Data Processing**: Converts each row into JSON format with fields:
   ```json
   {
     "date": "2024-01-15",
     "symbol": "ES1 Index", 
     "price": 1270.57,
     "timestamp": "2024-01-15 09:30:00"
   }
   ```
3. **Kafka Streaming**: Sends each record to `futures-ticks` topic at high frequency
4. **Real-time Simulation**: Creates a live market data feed effect

---

### **Phase 2: Real Technical Indicator Calculation**
```
Kafka: futures-ticks → real_feature_factory.py → Kafka: real-features-ticks
```

**What happens in `real_feature_factory.py`:**

#### **State Management**
- **Price History**: Maintains `deque(maxlen=100)` for each symbol to store historical prices
- **Symbol Isolation**: Each symbol has its own price history for accurate calculations
- **Memory Efficiency**: Uses `defaultdict` to automatically create history for new symbols

#### **Technical Indicator Calculations**

**1. Moving Averages:**
```python
# Simple Moving Average (SMA)
def calculate_sma(self, prices, window):
    if len(prices) < window:
        return None
    return sum(list(prices)[-window:]) / window

# Exponential Moving Average (EMA)  
def calculate_ema(self, prices, window):
    alpha = 2.0 / (window + 1)
    ema = prices_list[0]
    for price in prices_list[1:]:
        ema = alpha * price + (1 - alpha) * ema
    return ema
```

**2. RSI (Relative Strength Index):**
```python
def calculate_rsi(self, prices, window=14):
    # Calculate price changes
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    
    # Separate gains and losses
    gains = [delta if delta > 0 else 0 for delta in deltas]
    losses = [-delta if delta < 0 else 0 for delta in deltas]
    
    # Calculate average gain and loss
    avg_gain = sum(gains[-window:]) / window
    avg_loss = sum(losses[-window:]) / window
    
    # RSI formula
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi
```

**3. MACD (Moving Average Convergence Divergence):**
```python
def calculate_macd(self, prices, fast=12, slow=26, signal=9):
    # Calculate fast and slow EMAs
    ema_fast = self.calculate_ema(prices, fast)
    ema_slow = self.calculate_ema(prices, slow)
    
    # MACD line
    macd_line = ema_fast - ema_slow
    
    # Signal line (EMA of MACD)
    signal_line = self.calculate_ema(macd_history, signal)
    
    # Histogram
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram
```

**4. Bollinger Bands:**
```python
def calculate_bollinger_bands(self, prices, window=20, num_std=2):
    # Calculate SMA
    sma = sum(prices_list[-window:]) / window
    
    # Calculate standard deviation
    variance = sum((price - sma) ** 2 for price in prices_list[-window:]) / window
    std_dev = math.sqrt(variance)
    
    # Upper and lower bands
    upper_band = sma + (num_std * std_dev)
    lower_band = sma - (num_std * std_dev)
    return upper_band, sma, lower_band
```

**5. Volatility:**
```python
def calculate_volatility(self, prices, window=20):
    # Calculate returns
    returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
    
    # Annualized volatility
    return np.std(returns) * math.sqrt(252)
```

#### **Feature Output**
Each tick generates a comprehensive feature set:
```json
{
  "symbol": "ES1 Index",
  "current_price": 1270.57,
  "timestamp": "2024-01-15 09:30:00",
  "processing_time": "2024-01-15T09:30:00.123456",
  "sma_5": 1268.45,
  "sma_10": 1265.32,
  "sma_20": 1260.18,
  "ema_12": 1269.87,
  "ema_26": 1262.45,
  "rsi_14": 67.40,
  "macd_line": -2.7492,
  "macd_signal": -1.2345,
  "macd_histogram": -1.5147,
  "bb_upper": 1285.67,
  "bb_middle": 1260.18,
  "bb_lower": 1234.69,
  "volatility_20": 0.150,
  "momentum_10": 0.0234,
  "price_change": 2.45,
  "price_change_pct": 0.193,
  "lag_1": 1268.12,
  "lag_2": 1265.89,
  "lag_5": 1258.76
}
```

---

### **Phase 3: Machine Learning Prediction**
```
Kafka: real-features-ticks → real_ml_consumer.py → SQLite Database
```

**What happens in `real_ml_consumer.py`:**

#### **Model Loading & Training**
```python
class RealMLPredictor:
    def __init__(self):
        try:
            # Load pre-trained models
            self.xgboost_model = joblib.load('xgboost_model.joblib')
            self.randomforest_model = joblib.load('randomforest_model.joblib')
            # ... other models
        except:
            # Train new models if files don't exist
            self.train_new_models()
```

#### **Feature Vector Preparation**
```python
def predict(self, features):
    # Extract features in correct order for ML models
    feature_vector = np.array([
        features.get('rsi_14', 50),           # RSI
        features.get('macd_line', 0),         # MACD
        features.get('volatility_20', 0.2),   # Volatility
        features.get('momentum_10', 0),       # Momentum
        features.get('price_change_pct', 0),  # Price Change %
        features.get('current_price', 100)    # Current Price
    ]).reshape(1, -1)
```

#### **ML Model Predictions**
```python
# Make predictions with all models
predictions = {
    'XGBoost': self.xgboost_model.predict(feature_vector)[0],
    'RandomForest': self.randomforest_model.predict(feature_vector)[0],
    'AdaBoost': self.adaboost_model.predict(feature_vector)[0],
    'LogisticReg': self.logreg_model.predict(feature_vector)[0]
}

# Convert to UP/DOWN signals
return {k: "UP" if v == 1 else "DOWN" for k, v in predictions.items()}
```

#### **Rule-Based Predictions**
```python
def calculate_simple_rules(self, features):
    rules = {}
    
    # RSI Rule
    rsi = features.get('rsi_14', 50)
    rules['RSI_Rule'] = "UP" if rsi > 50 else "DOWN"
    
    # MACD Rule
    macd = features.get('macd_line', 0)
    rules['MACD_Rule'] = "UP" if macd > 0 else "DOWN"
    
    # Momentum Rule
    momentum = features.get('momentum_10', 0)
    rules['Momentum_Rule'] = "UP" if momentum > 0 else "DOWN"
    
    # Price Change Rule
    price_change = features.get('price_change_pct', 0)
    rules['PriceChange_Rule'] = "UP" if price_change > 0 else "DOWN"
    
    # Bollinger Bands Rule
    current_price = features.get('current_price', 100)
    bb_upper = features.get('bb_upper', current_price)
    bb_lower = features.get('bb_lower', current_price)
    
    if current_price > bb_upper:
        rules['BB_Rule'] = "DOWN"  # Overbought
    elif current_price < bb_lower:
        rules['BB_Rule'] = "UP"    # Oversold
    else:
        rules['BB_Rule'] = "NEUTRAL"
    
    return rules
```

#### **Database Storage**
```python
# Combine all predictions and features
output_data = {
    'symbol': features['symbol'],
    'current_price': features['current_price'],
    'timestamp': features['timestamp'],
    'processing_time': datetime.now().isoformat(),
    'rsi_14': features.get('rsi_14'),
    'macd_line': features.get('macd_line'),
    'volatility_20': features.get('volatility_20'),
    'momentum_10': features.get('momentum_10'),
    'price_change_pct': features.get('price_change_pct'),
    'sma_10': features.get('sma_10'),
    'sma_20': features.get('sma_20'),
    'bb_upper': features.get('bb_upper'),
    'bb_lower': features.get('bb_lower'),
    **ml_predictions,    # XGBoost, RandomForest, AdaBoost, LogisticReg
    **rule_predictions   # RSI_Rule, MACD_Rule, Momentum_Rule, etc.
}

# Write to SQLite database
write_to_sqlite(output_data)
```

---

### **Phase 4: Real-Time Dashboard Visualization**
```
SQLite Database → dashboard.py → Web Interface
```

**What happens in `dashboard.py`:**

#### **Data Retrieval**
```python
@st.cache_data(ttl=REFRESH_INTERVAL_SECONDS)
def get_data_for_symbol(symbol):
    with sqlite3.connect(DB_PATH) as conn:
        query = f"SELECT * FROM {TABLE_NAME} WHERE symbol = ? ORDER BY processing_time DESC LIMIT {DATA_LIMIT}"
        df = pd.read_sql_query(query, conn, params=(symbol,))
    return df
```

#### **Real-Time KPIs**
```python
# Current Price
kpi1.metric(label="💰 CURRENT PRICE", value=f"${latest_data['current_price']:,.2f}")

# RSI with color coding
rsi_val = latest_data.get('rsi_14')
if rsi_val is not None:
    rsi_color = "#00FF00" if rsi_val > 70 else "#FF0000" if rsi_val < 30 else "#FFFF00"
    kpi2.markdown(f"<div style='color: {rsi_color};'>{rsi_val:.1f}</div>")

# MACD with color coding
macd_val = latest_data.get('macd_line')
if macd_val is not None:
    macd_color = "#00FF00" if macd_val > 0 else "#FF0000"
    kpi3.markdown(f"<div style='color: {macd_color};'>{macd_val:.4f}</div>")
```

#### **Technical Charts**
```python
# Price line with moving averages
fig.add_trace(go.Scatter(x=df['processing_time'], y=df['current_price'], 
                        mode='lines', line_color='cyan', name='Price'))

# SMA overlays
if 'sma_10' in df.columns:
    fig.add_trace(go.Scatter(x=df['processing_time'], y=df['sma_10'], 
                            mode='lines', line_color='yellow', name='SMA (10)'))

# Bollinger Bands
if 'bb_upper' in df.columns:
    fig.add_trace(go.Scatter(x=df['processing_time'], y=df['bb_upper'], 
                            mode='lines', line_color='rgba(255,255,255,0.5)', name='BB Upper'))
```

#### **ML Predictions Display**
```python
def display_signal(model_name, signal):
    if signal == "UP":
        color = "#00FF00"
        arrow = "▲"
    elif signal == "DOWN":
        color = "#FF0000"
        arrow = "▼"
    else:
        color = "#FFFF00"
        arrow = "●"
    
    st.markdown(f"**{model_name}** <span style='color:{color};'>{arrow} {signal}</span>")
```

---

## ⚡ **Real-Time Performance Characteristics**

### **Data Flow Timing**
1. **Producer**: ~100ms per tick
2. **Feature Factory**: ~50ms per tick (technical calculations)
3. **ML Consumer**: ~30ms per tick (model inference)
4. **Dashboard**: 3-second refresh cycle

### **Memory Management**
- **Price History**: 100 prices per symbol (deque with maxlen)
- **Symbol Tracking**: 39 unique symbols
- **Database**: SQLite with automatic indexing
- **Cache**: Streamlit caching for performance

### **Scalability Features**
- **Symbol Isolation**: Each symbol processed independently
- **Stateful Processing**: Maintains historical context
- **Kafka Buffering**: Handles data spikes
- **Database Batching**: Efficient writes

### **Error Handling**
- **None Values**: Graceful handling of insufficient data
- **Model Loading**: Automatic retraining if models missing
- **Database Errors**: Schema validation and reset
- **Kafka Reconnection**: Automatic recovery

This workflow creates a **professional-grade real-time trading system** that processes live market data, calculates accurate technical indicators, applies machine learning models, and presents everything in a beautiful, interactive dashboard! 🚀

## 🎯 Performance Metrics

- **Real-time Processing**: Updates every 3 seconds
- **Data Volume**: 11,000+ records processed
- **Symbol Coverage**: 39 unique financial instruments
- **Technical Accuracy**: Proper window functions for all indicators
- **ML Performance**: 4 trained models with real feature engineering

## 🔧 Troubleshooting

### Common Issues
1. **Kafka Connection**: Ensure Kafka is running (`brew services start kafka`)
2. **Database Errors**: Delete `predictions.db` to reset schema
3. **Model Loading**: Old model files are automatically retrained
4. **Dashboard Not Loading**: Check if all components are running

### Logs to Monitor
- Feature Factory: Technical indicator calculations
- ML Consumer: Model predictions and database writes
- Dashboard: Real-time data refresh

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

**Flameback Capital** - Professional-grade real-time trading analytics powered by actual technical indicators and machine learning. 