import pandas as pd
import numpy as np

print("Starting data preparation...")

# --- Step 1: Load the raw time-series data ---
try:
    df_wide = pd.read_csv('Seas Port Final.csv')
    print("Successfully loaded 'Seas Port Final.csv'.")
except FileNotFoundError:
    print("Error: 'Seas Port Final.csv' not found. Make sure it's in the same directory.")
    exit()

# --- Step 2: Reshape the data from wide to long format ---
# This is the most crucial step. We are un-pivoting the data.
df_long = df_wide.melt(id_vars=['Dates'], var_name='symbol', value_name='price')
print(f"Reshaped data from wide to long format. Total rows: {len(df_long)}")

# --- Step 3: Clean the data ---
# Rename 'Dates' to a more standard name
df_long.rename(columns={'Dates': 'date'}, inplace=True)

# The file contains '#N/A N/A' as strings for missing values. Replace them with NaN.
df_long.replace('#N/A N/A', np.nan, inplace=True)

# Convert price to a numeric type. If any value can't be converted, it becomes NaN.
df_long['price'] = pd.to_numeric(df_long['price'], errors='coerce')

# Drop all rows that have any missing values (NaN)
df_long.dropna(inplace=True)
print(f"Cleaned data. Rows after dropping missing values: {len(df_long)}")

# Convert the 'date' column to a proper datetime object
df_long['date'] = pd.to_datetime(df_long['date'])

# --- Step 4: Sort the data chronologically ---
# This is essential for simulating a realistic time-series stream.
df_long.sort_values(by='date', inplace=True)
print("Sorted data by date.")

# --- Step 5: Save the prepared data to a new CSV file ---
output_filename = 'timeseries_data_long.csv'
df_long.to_csv(output_filename, index=False)

print("-" * 30)
print(f"Success! Cleaned data has been saved to '{output_filename}'")
print("Here's a preview of the final data:")
print(df_long.head())
print("-" * 30)