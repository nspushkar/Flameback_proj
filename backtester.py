# ... (FinancialBacktester class and get_data_from_db function are the same as before) ...
import pandas as pd
import sqlite3
from datetime import datetime
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "predictions.db")
TABLE_NAME = "predictions"
INITIAL_CASH = 100000.0
OUTPUT_REPORT_FILE = os.path.join(SCRIPT_DIR, "financial_backtest_report.txt")

class FinancialBacktester:
    # ... (no changes needed in this class)
    def __init__(self, initial_cash):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.position = 0
        self.portfolio_value = initial_cash
        self.entry_price = 0
        self.transactions = []
        self.equity_curve = []
        print(f"✅ Financial Backtester initialized with ${initial_cash:,.2f} cash.")

    def run_simulation(self, historical_data, signal_column):
        if historical_data.empty:
            print("❌ No data found to backtest. Exiting.")
            return

        print(f"🚀 Starting backtest simulation on {len(historical_data)} data points...")
        print(f"   Using signal from column: '{signal_column}'")

        for index, row in historical_data.iterrows():
            signal = row[signal_column]
            price = row['current_price']
            timestamp = row['timestamp']
            
            if pd.isna(price) or price <= 0:
                continue

            if signal == "UP" and self.position == 0:
                self.position = self.cash / price
                self.entry_price = price
                self.cash = 0
                trade_log = f"{timestamp} | BUY   | {self.position:.4f} units @ ${price:,.2f}"
                self.transactions.append(trade_log)

            elif signal == "DOWN" and self.position > 0:
                self.cash = self.position * price
                pnl = self.cash - (self.position * self.entry_price)
                pnl_pct = (pnl / (self.position * self.entry_price)) * 100 if self.entry_price > 0 else 0
                trade_log = f"{timestamp} | SELL  | All units @ ${price:,.2f} | PnL: ${pnl:,.2f} ({pnl_pct:.2f}%)"
                self.transactions.append(trade_log)
                self.position = 0
                self.entry_price = 0
            
            current_portfolio_value = self.cash + (self.position * price)
            self.equity_curve.append(current_portfolio_value)

        self.portfolio_value = self.equity_curve[-1] if self.equity_curve else self.initial_cash
        print("✅ Simulation complete.")

    def generate_report(self):
        print("📝 Generating financial report...")
        final_value = self.portfolio_value
        total_pnl = final_value - self.initial_cash
        total_return_pct = (total_pnl / self.initial_cash) * 100 if self.initial_cash > 0 else 0
        equity_df = pd.DataFrame(self.equity_curve, columns=['value'])
        daily_returns = equity_df['value'].pct_change().dropna()
        sharpe_ratio = "N/A"
        if not daily_returns.empty and daily_returns.std() != 0:
            sharpe_ratio = (daily_returns.mean() / daily_returns.std()) * (252**0.5)
            sharpe_ratio = f"{sharpe_ratio:.2f}"

        report = [
            "=" * 60, "      FINANCIAL STRATEGY BACKTEST REPORT", "=" * 60,
            f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
            "--- PERFORMANCE METRICS ---",
            f"Initial Portfolio Value: ${self.initial_cash:,.2f}",
            f"Final Portfolio Value:   ${final_value:,.2f}",
            f"Total Net PnL:           ${total_pnl:,.2f}",
            f"Total Return:            {total_return_pct:.2f}%",
            f"Sharpe Ratio (Annualized): {sharpe_ratio}",
            f"Total Trades Executed:   {len(self.transactions)}\n",
            "--- TRANSACTION LOG ---"
        ]
        if self.transactions: report.extend(self.transactions)
        else: report.append("No trades were executed.")
        report.append("\n" + "=" * 60)
        
        report_str = "\n".join(report)
        try:
            with open(OUTPUT_REPORT_FILE, "w") as f: f.write(report_str)
            print(f"✅ Report saved to '{OUTPUT_REPORT_FILE}'")
        except Exception as e: print(f"❌ Could not write report file: {e}")

def get_data_from_db(symbol):
    """Fetches and sorts historical prediction data from the SQLite database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # New, more direct query.
            # We trust that the 'symbol' variable passed in is already cleaned.
            query = f"SELECT * FROM {TABLE_NAME} WHERE TRIM(UPPER(symbol)) = ? ORDER BY timestamp ASC"
            
            # The params argument expects a tuple, so we pass (symbol,)
            df = pd.read_sql_query(query, conn, params=(symbol,))
            
        return df
    except Exception as e:
        print(f"❌ Error reading from database: {e}")
        return pd.DataFrame()

# ==========================================================
# THIS IS THE MODIFIED main() FUNCTION FOR DIAGNOSIS
# ==========================================================
def main():
    """Main function to run the backtester."""
    print("--- FLAMEBACK OFFLINE BACKTESTING ENGINE (ULTRA-DEBUG MODE) ---")
    
    try:
        if not os.path.exists(DB_PATH):
            print(f"❌ FATAL ERROR: Database file not found at path: {DB_PATH}")
            return
            
        with sqlite3.connect(DB_PATH) as conn:
            all_db_symbols_raw = pd.read_sql_query(f"SELECT DISTINCT symbol FROM {TABLE_NAME}", conn)['symbol'].tolist()

        if not all_db_symbols_raw:
            print("❌ The database is empty or the 'predictions' table has no data.")
            return

        # --- DIAGNOSTIC STEP 1: Clean and print the symbols ---
        # We will trim whitespace and convert to uppercase to normalize everything
        all_db_symbols_clean = sorted([s.strip().upper() for s in all_db_symbols_raw])
        
        print("\n[DEBUG] Cleaned symbols found in DB:", all_db_symbols_clean)
        
        target_symbol_input = input("Enter the symbol you want to backtest: ")
        
        # Clean the user input in the exact same way
        target_symbol_clean = target_symbol_input.strip().upper()

        # --- DIAGNOSTIC STEP 2: Compare the cleaned versions ---
        print(f"[DEBUG] Your cleaned input: '{target_symbol_clean}'")
        print(f"[DEBUG] List of cleaned symbols from DB: {all_db_symbols_clean}")
        
        if target_symbol_clean not in all_db_symbols_clean:
            print(f"\n❌ Error: Cleaned symbol '{target_symbol_clean}' not found in the list of cleaned available symbols.")
            
            # --- DIAGNOSTIC STEP 3: Show the raw values for comparison ---
            print("\n[DEBUG] To help diagnose, here are the RAW, UNCLEANED symbols from the database:")
            for i, raw_symbol in enumerate(all_db_symbols_raw):
                # We use repr() to make whitespace and special characters visible
                print(f"  {i+1}: {repr(raw_symbol)}")
            return
        
        print(f"\n✅ Match found! Proceeding to fetch data for '{target_symbol_clean}'...")

    except Exception as e:
        print(f"❌ FATAL ERROR: Could not fetch symbols from database: {e}")
        return

    # Fetch the historical data using the cleaned symbol
    historical_data = get_data_from_db(target_symbol_clean)

    if historical_data.empty:
        print(f"❌ Error: The query for symbol '{target_symbol_clean}' returned no data, even though the symbol exists. Check for data integrity issues.")
        return

    # ... The rest of the script is the same ...
    all_columns = historical_data.columns.tolist()
    print("\nColumns available in the DataFrame:", all_columns)
    
    print("Please choose a signal column to use for the backtest.")
    signal_column = input(f"Enter one of {all_columns}: ").strip()
    
    if signal_column not in all_columns:
        print(f"❌ Error: Column '{signal_column}' not found in the data.")
        return
        
    backtester = FinancialBacktester(initial_cash=INITIAL_CASH)
    backtester.run_simulation(historical_data, signal_column)
    backtester.generate_report()

if __name__ == "__main__":
    main()