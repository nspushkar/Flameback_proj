import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="FLAMEBACK | Real-Time Terminal",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS for Bloomberg Terminal Look ---
st.markdown("""
<style>
    /* Main background and text */
    .main {
        background-color: #000000;
        color: #E0E0E0;
    }
    /* Metric styling */
    .st-emotion-cache-1r6slb0 {
        border: 1px solid #333;
        border-radius: 5px;
        padding: 10px;
    }
    .st-emotion-cache-1r6slb0 .st-emotion-cache-1g6goon { /* Metric Label */
        color: #B0B0B0;
        font-size: 0.9rem;
    }
    .st-emotion-cache-1r6slb0 .st-emotion-cache-ocqkz7 { /* Metric Value */
        color: #FFFFFF;
        font-size: 1.5rem;
    }
    /* Custom classes for signal colors */
    .signal-up { color: #00FF00; }
    .signal-down { color: #FF0000; }
    /* Sidebar styling */
    .st-emotion-cache-6q9sum {
        background-color: #101010;
    }
</style>
""", unsafe_allow_html=True)

# --- Database & Refresh Config ---
DB_PATH = "predictions.db"
TABLE_NAME = "predictions"
DATA_LIMIT = 150
REFRESH_INTERVAL_SECONDS = 2
st_autorefresh(interval=REFRESH_INTERVAL_SECONDS * 1000, key="data_refresher")

# --- Helper Functions (Cached for performance) ---
@st.cache_data(ttl=5)
def get_symbols():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            symbols = pd.read_sql_query(f"SELECT DISTINCT symbol FROM {TABLE_NAME}", conn)
        return sorted(symbols['symbol'].tolist())
    except:
        return []

@st.cache_data(ttl=REFRESH_INTERVAL_SECONDS)
def get_data_for_symbol(symbol):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            query = f"SELECT * FROM (SELECT * FROM {TABLE_NAME} WHERE symbol = ? ORDER BY date DESC LIMIT {DATA_LIMIT}) ORDER BY date ASC"
            df = pd.read_sql_query(query, conn, params=(symbol,))
        df['date'] = pd.to_datetime(df['date'])
        return df
    except:
        return pd.DataFrame()

# --- UI Functions ---
def display_signal(model_name, signal):
    color = "green" if signal == "UP" else "red"
    arrow = "▲" if signal == "UP" else "▼"
    st.markdown(f"**{model_name}** <span style='color:{color}; font-size: 1.2em;'>{arrow} {signal}</span>", unsafe_allow_html=True)

# --- Sidebar ---
st.sidebar.title("FLAMEBACK TERMINAL")
symbols = get_symbols()
if not symbols:
    st.warning("Awaiting data stream... Please ensure all pipeline components are running.")
    st.stop()
selected_symbol = st.sidebar.selectbox("SELECT SYMBOL", symbols)
st.sidebar.info(f"Dashboard refreshes every {REFRESH_INTERVAL_SECONDS} seconds.")
st.sidebar.text(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

# --- Main Dashboard ---
st.header(f"Live Analysis for: {selected_symbol}")
df = get_data_for_symbol(selected_symbol)

if not df.empty:
    latest_data = df.iloc[-1]
    
    # --- KPI Row ---
    price_change = latest_data['current_price'] * latest_data['simple_return']
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric(label="LAST PRICE", value=f"${latest_data['current_price']:,.2f}", delta=f"{price_change:,.3f}")
    kpi2.metric(label="VOLATILITY (10D)", value=f"{latest_data['volatility_10']:.3f}")
    kpi3.metric(label="RSI (14D)", value=f"{latest_data['rsi']:.2f}")
    
    # --- Live Price Chart with Indicators ---
    fig = go.Figure()
    # Bollinger Bands Area
    fig.add_trace(go.Scatter(x=df['date'], y=df['bb_upper'], fill=None, mode='lines', line_color='rgba(255,255,255,0.2)', name='Bollinger Upper'))
    fig.add_trace(go.Scatter(x=df['date'], y=df['bb_lower'], fill='tonexty', mode='lines', line_color='rgba(255,255,255,0.2)', name='Bollinger Lower'))
    # Moving Averages
    fig.add_trace(go.Scatter(x=df['date'], y=df['sma_10'], mode='lines', line_color='yellow', name='SMA 10'))
    fig.add_trace(go.Scatter(x=df['date'], y=df['sma_20'], mode='lines', line_color='purple', name='SMA 20'))
    # Main Price Line (plotted last to be on top)
    fig.add_trace(go.Scatter(x=df['date'], y=df['current_price'], mode='lines', line_color='cyan', name='Price', line=dict(width=3)))

    fig.update_layout(
        title_text=f'Price Action & Technical Indicators', template='plotly_dark',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Prediction Signals Row ---
    st.subheader("MODEL PREDICTION SIGNALS")
    cols = st.columns(5)
    with cols[0]:
        display_signal("Momentum", latest_data["Momentum"])
    with cols[1]:
        display_signal("XGBoost", latest_data["XGBoost"])
    with cols[2]:
        display_signal("RandomForest", latest_data["RandomForest"])
    with cols[3]:
        display_signal("AdaBoost", latest_data["AdaBoost"])
    with cols[4]:
        display_signal("LogisticReg", latest_data["LogisticReg"])
else:
    st.info(f"Waiting for new data for symbol: {selected_symbol}...")