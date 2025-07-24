import pandas as pd
import numpy as np
import math
import joblib

# Import all the models we want to train
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
import xgboost as xgb

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

print("Starting UNIFIED model training script...")

# --- Step 1: Load Historical Data ---
df = pd.read_csv('timeseries_data_long.csv', parse_dates=['date'])
df.sort_values(by=['symbol', 'date'], inplace=True)
print(f"Loaded {len(df)} historical data points.")

# --- Step 2: Re-implement Your Stateless Feature Logic in Pandas ---
print("Applying stateless feature estimation logic...")

def price_change_indicator(price):
    last_digit = int(price * 100) % 100
    return (last_digit - 50) / 1000.0

def lag_feature_estimate(price, offset):
    adjustment = np.sin(price * 0.01 + offset) * (price * 0.001)
    return price + adjustment

df['simple_return'] = df['price'].apply(price_change_indicator)
df['lag_1'] = df.apply(lambda row: lag_feature_estimate(row['price'], 1), axis=1)

# --- Step 3: Create the Target Variable ---
df['actual_next_price'] = df.groupby('symbol')['price'].shift(-1)
df.dropna(inplace=True)
df['target'] = np.where(df['actual_next_price'] > df['price'], 1, 0)
print("Target variable created.")

# --- Step 4: Prepare Data for Training ---
feature_columns = ['simple_return', 'lag_1', 'price']
X = df[feature_columns]
y = df['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
print(f"Data split into {len(X_train)} training and {len(X_test)} testing samples.")

# --- Step 5: Define, Train, and Evaluate All Models ---

models = {
    "LogisticRegression": LogisticRegression(random_state=42, max_iter=1000),
    "RandomForest": RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
    "AdaBoost": AdaBoostClassifier(n_estimators=100, random_state=42),
    "XGBoost": xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss', n_jobs=-1)
}

model_performance = {}

for name, model in models.items():
    print("\n" + "="*20 + f" Training {name} " + "="*20)
    
    # Train the model
    model.fit(X_train, y_train)
    
    # Evaluate the model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    model_performance[name] = accuracy
    
    print(f"Model: {name}")
    print(f"Accuracy: {accuracy:.4f}")
    print(classification_report(y_test, y_pred))
    
    # Save the trained model to a file
    model_filename = f"{name.lower()}_model.joblib"
    joblib.dump(model, model_filename)
    print(f"Model saved to '{model_filename}'")

# --- Step 6: Final Performance Summary ---
print("\n" + "="*20 + " Final Model Comparison " + "="*20)
for name, accuracy in model_performance.items():
    print(f"{name:<20} | Accuracy: {accuracy:.4f}")
print("="*57)