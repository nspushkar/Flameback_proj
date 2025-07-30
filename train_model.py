import pandas as pd
import numpy as np
import math
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib

print("Starting OFFLINE model training script...")

# --- Step 1: Load Historical Data ---
df = pd.read_csv('timeseries_data_long.csv', parse_dates=['date'])
df.sort_values(by=['symbol', 'date'], inplace=True)
print(f"Loaded {len(df)} historical data points.")

# --- Step 2: Re-implement Your Feature Logic in Pandas ---
# This MUST match the logic in your stateless_feature_factory.py
print("Applying stateless feature estimation logic...")

def price_change_indicator(price):
    last_digit = int(price * 100) % 100
    return (last_digit - 50) / 1000.0

def lag_feature_estimate(price, offset):
    adjustment = np.sin(price * 0.01 + offset) * (price * 0.001)
    return price + adjustment

df['simple_return'] = df['price'].apply(price_change_indicator)
df['lag_1'] = df.apply(lambda row: lag_feature_estimate(row['price'], 1), axis=1)
# Add any other stateless features you want the model to learn from
# For this example, we'll keep it simple to train on 'simple_return' and 'lag_1'

# --- Step 3: Create the Target Variable ---
# Predict if the REAL next day's price will be up (1) or down (0)
df['actual_next_price'] = df.groupby('symbol')['price'].shift(-1)
df.dropna(inplace=True) # Drop last row for each symbol
df['target'] = np.where(df['actual_next_price'] > df['price'], 1, 0)
print("Target variable created.")

# --- Step 4: Prepare Data and Train Model ---
feature_columns = ['simple_return', 'lag_1', 'price'] # Features model will use
X = df[feature_columns]
y = df['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

print("Training XGBoost Classifier...")
model = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')
model.fit(X_train, y_train)

# --- Step 5: Evaluate and Save Model ---
print("Evaluating model...")
preds = model.predict(X_test)
print(f"Model Accuracy: {accuracy_score(y_test, preds):.4f}")

model_filename = 'xgboost_stateless_model.joblib'
joblib.dump(model, model_filename)
print("-" * 40)
print(f"SUCCESS: Model trained and saved to '{model_filename}'")
print("-" * 40)