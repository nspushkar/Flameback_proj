# ... (The RealMLPredictor class and calculate_simple_rules function are the same as the previous correct version) ...
# ... I will include them here for a complete, copy-pastable file ...

import json
import joblib
import numpy as np
import pandas as pd
import sqlite3
from datetime import datetime
from kafka import KafkaConsumer
from collections import deque
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression
import xgboost as xgb

# --- Configuration ---
RE_TRAIN_INTERVAL = 100
DATA_BUFFER_MAXLEN = 5000

class RealMLPredictor:
    def __init__(self):
        self.models = {
            'XGBoost': None, 'RandomForest': None,
            'AdaBoost': None, 'LogisticReg': None
        }
        self.data_buffers = {}
        self.message_counters = {}
        self.load_models()

    def load_models(self):
        for name in self.models.keys():
            try:
                self.models[name] = joblib.load(f'{name.lower()}_model.joblib')
                print(f"✅ Loaded pre-trained model: {name}")
            except FileNotFoundError:
                print(f"⚠️ {name} model not found. It will be trained live once enough data is collected.")

    def _prepare_data_for_training(self, symbol):
        if symbol not in self.data_buffers or len(self.data_buffers[symbol]) < 50:
            return None, None
        df = pd.DataFrame(list(self.data_buffers[symbol]))
        df['price_change_next_tick'] = df['current_price'].shift(-1) - df['current_price']
        df['y'] = (df['price_change_next_tick'] > 0).astype(int)
        df.dropna(subset=['y'], inplace=True)
        if df.empty or len(np.unique(df['y'])) < 2:
            return None, None
        feature_cols = [
            'rsi_14', 'macd_line', 'volatility_20', 'momentum_10',
            'price_change_pct', 'current_price'
        ]
        for col in feature_cols:
            if col not in df.columns: df[col] = 0
            df[col].fillna(df[col].mean(), inplace=True)
        return df[feature_cols].values, df['y'].values

    def online_train(self, symbol):
        print(f"\n🔄 [Training] Re-training models for symbol: {symbol}...")
        X, y = self._prepare_data_for_training(symbol)
        if X is None:
            print("   ...Not enough diverse data to train. Skipping.")
            return
        print(f"   Training with {len(X)} data points.")
        self.models['XGBoost'] = xgb.XGBClassifier(random_state=42, use_label_encoder=False, eval_metric='logloss')
        self.models['RandomForest'] = RandomForestClassifier(random_state=42)
        self.models['AdaBoost'] = AdaBoostClassifier(random_state=42)
        self.models['LogisticReg'] = LogisticRegression(random_state=42)
        for name, model in self.models.items():
            model.fit(X, y)
            print(f"   ✅ {name} model re-trained for {symbol}.")
        print("-" * 40)

    def predict(self, features):
        if any(model is None for model in self.models.values()):
            return {name: "TRAINING" for name in self.models.keys()}
        feature_vector = np.array([
            features.get(key, 0) or 0 for key in [
                'rsi_14', 'macd_line', 'volatility_20', 'momentum_10',
                'price_change_pct', 'current_price'
            ]
        ]).reshape(1, -1)
        predictions = {}
        for name, model in self.models.items():
            pred = model.predict(feature_vector)[0]
            predictions[name] = "UP" if pred == 1 else "DOWN"
        return predictions

    def process_new_feature_message(self, features):
        symbol = features['symbol']
        if symbol not in self.data_buffers:
            self.data_buffers[symbol] = deque(maxlen=DATA_BUFFER_MAXLEN)
            self.message_counters[symbol] = 0
        self.data_buffers[symbol].append(features)
        self.message_counters[symbol] += 1
        if self.message_counters[symbol] % RE_TRAIN_INTERVAL == 0:
            self.online_train(symbol)
        return self.predict(features)

def calculate_simple_rules(features):
    rules = {}
    rsi = features.get('rsi_14', 50) or 50
    rules['RSI_Rule'] = "UP" if rsi > 50 else "DOWN"
    macd = features.get('macd_line', 0) or 0
    rules['MACD_Rule'] = "UP" if macd > 0 else "DOWN"
    momentum = features.get('momentum_10', 0) or 0
    rules['Momentum_Rule'] = "UP" if momentum > 0 else "DOWN"
    price_change = features.get('price_change_pct', 0) or 0
    rules['PriceChange_Rule'] = "UP" if price_change > 0 else "DOWN"
    current_price = features.get('current_price', 100) or 100
    bb_upper = features.get('bb_upper')
    bb_lower = features.get('bb_lower')
    if bb_upper is None or bb_lower is None:
        rules['BB_Rule'] = "NEUTRAL"
    elif current_price > bb_upper:
        rules['BB_Rule'] = "DOWN"
    elif current_price < bb_lower:
        rules['BB_Rule'] = "UP"
    else:
        rules['BB_Rule'] = "NEUTRAL"
    return rules

# =================================================================
# CORRECTED DATABASE WRITING LOGIC STARTS HERE
# =================================================================

def write_to_sqlite(data, db_path="predictions.db", table_name="predictions"):
    """Write prediction data to SQLite database."""
    try:
        with sqlite3.connect(db_path) as conn:
            # This single line is robust. It creates a DataFrame from a list
            # containing one dictionary. Pandas handles the column alignment.
            df = pd.DataFrame([data])
            df.to_sql(table_name, conn, if_exists="append", index=False)
        
        # Using a modulo on the counter to prevent console spamming
        if data.get('message_counters', {}).get(data['symbol'], 1) % 20 == 1:
            print(f"💾 Wrote prediction for {data['symbol']} to database")
    except Exception as e:
        # Provide more detail on the error
        print(f"❌ Error writing to database: {e}")
        print(f"   Problematic data: {data}")


def main():
    predictor = RealMLPredictor()
    
    consumer = KafkaConsumer(
        'real-features-ticks',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    
    print("🤖 REAL ML PREDICTION ENGINE (WITH LIVE TRAINING) STARTED")
    print("=" * 60)
    
    try:
        for message in consumer:
            features = message.value
            
            # This handles both training and prediction logic
            ml_predictions = predictor.process_new_feature_message(features)
            
            # The rest is the same as your original code
            rule_predictions = calculate_simple_rules(features)
            
            # Combine all predictions
            all_predictions = {**ml_predictions, **rule_predictions}
            
            # --- THIS IS THE CORRECTED, ORIGINAL DATA ASSEMBLY LOGIC ---
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
                **all_predictions
            }
            # --- END OF CORRECTED LOGIC ---
            
            # Write to database
            write_to_sqlite(output_data)
            
            # Print results to console (as in original code)
            print(f"📊 {features['symbol']}: ${features['current_price']:.2f}")
            
            rsi_val = features.get('rsi_14')
            macd_val = features.get('macd_line')
            
            rsi_str = f"{rsi_val:.1f}" if rsi_val is not None else "N/A"
            macd_str = f"{macd_val:.4f}" if macd_val is not None else "N/A"
            
            print(f"   RSI: {rsi_str} | MACD: {macd_str}")
            print(f"   ML: {ml_predictions} | Rules: {rule_predictions}")
            print("-" * 40)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping ML prediction engine...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()