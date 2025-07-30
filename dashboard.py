import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="FLAMEBACK | Real-Time Trading Terminal",
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
    .signal-neutral { color: #FFFF00; }
    /* Sidebar styling */
    .st-emotion-cache-6q9sum {
        background-color: #101010;
    }
</style>
""", unsafe_allow_html=True)

# --- Database & Refresh Config ---
DB_PATH = "predictions.db"
TABLE_NAME = "predictions"
DATA_LIMIT = 100
REFRESH_INTERVAL_SECONDS = 3
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
            query = f"SELECT * FROM (SELECT * FROM {TABLE_NAME} WHERE symbol = ? ORDER BY processing_time DESC LIMIT {DATA_LIMIT}) ORDER BY processing_time ASC"
            df = pd.read_sql_query(query, conn, params=(symbol,))
        df['processing_time'] = pd.to_datetime(df['processing_time'])
        return df
    except:
        return pd.DataFrame()

# --- UI Functions ---
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
    
    st.markdown(f"**{model_name}** <span style='color:{color}; font-size: 1.2em;'>{arrow} {signal}</span>", unsafe_allow_html=True)

def display_technical_indicator(name, value, format_str="{:.2f}"):
    if value is not None and not pd.isna(value):
        st.metric(name, format_str.format(value))
    else:
        st.metric(name, "N/A")

# --- Sidebar ---
st.sidebar.title("FLAMEBACK TERMINAL")
st.sidebar.markdown("**Real-Time ML Trading System**")

symbols = get_symbols()
if not symbols:
    st.warning("Awaiting data stream... Please ensure all pipeline components are running.")
    st.stop()

selected_symbol = st.sidebar.selectbox("SELECT SYMBOL", symbols)
st.sidebar.info(f"Dashboard refreshes every {REFRESH_INTERVAL_SECONDS} seconds.")
st.sidebar.text(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")


# --- Main Dashboard ---
st.header(f"🚀 Live Trading Analysis: {selected_symbol}")
df = get_data_for_symbol(selected_symbol)

if not df.empty:
    latest_data = df.iloc[-1]
    
    # --- KPI Row ---
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    kpi1.metric(label="💰 CURRENT PRICE", value=f"${latest_data['current_price']:,.2f}")
    
    # RSI
    rsi_val = latest_data.get('rsi_14')
    if rsi_val is not None and not pd.isna(rsi_val):
        rsi_color = "#00FF00" if rsi_val > 70 else "#FF0000" if rsi_val < 30 else "#FFFF00"
        kpi2.markdown(f"<div style='text-align: center;'><div style='color: #B0B0B0; font-size: 0.9rem;'>RSI (14)</div><div style='color: {rsi_color}; font-size: 1.5rem; font-weight: bold;'>{rsi_val:.1f}</div></div>", unsafe_allow_html=True)
    else:
        kpi2.metric(label="RSI (14)", value="N/A")
    
    # MACD
    macd_val = latest_data.get('macd_line')
    if macd_val is not None and not pd.isna(macd_val):
        macd_color = "#00FF00" if macd_val > 0 else "#FF0000"
        kpi3.markdown(f"<div style='text-align: center;'><div style='color: #B0B0B0; font-size: 0.9rem;'>MACD</div><div style='color: {macd_color}; font-size: 1.5rem; font-weight: bold;'>{macd_val:.4f}</div></div>", unsafe_allow_html=True)
    else:
        kpi3.metric(label="MACD", value="N/A")
    
    # Volatility
    vol_val = latest_data.get('volatility_20')
    if vol_val is not None and not pd.isna(vol_val):
        kpi4.metric(label="📊 VOLATILITY (20)", value=f"{vol_val:.3f}")
    else:
        kpi4.metric(label="VOLATILITY (20)", value="N/A")
    
    # --- Technical Indicators Chart ---
    st.subheader("📈 Technical Indicators")
    
    fig = go.Figure()
    
    # Price line
    fig.add_trace(go.Scatter(
        x=df['processing_time'], 
        y=df['current_price'], 
        mode='lines', 
        line_color='cyan', 
        name='Price', 
        line=dict(width=3)
    ))
    
    # SMA lines
    if 'sma_10' in df.columns:
        sma_10_data = df['sma_10'].dropna()
        if not sma_10_data.empty:
            fig.add_trace(go.Scatter(
                x=df.loc[sma_10_data.index, 'processing_time'],
                y=sma_10_data,
                mode='lines',
                line_color='yellow',
                name='SMA (10)',
                line=dict(width=2)
            ))
    
    if 'sma_20' in df.columns:
        sma_20_data = df['sma_20'].dropna()
        if not sma_20_data.empty:
            fig.add_trace(go.Scatter(
                x=df.loc[sma_20_data.index, 'processing_time'],
                y=sma_20_data,
                mode='lines',
                line_color='orange',
                name='SMA (20)',
                line=dict(width=2)
            ))
    
    # Bollinger Bands
    if 'bb_upper' in df.columns and 'bb_lower' in df.columns:
        bb_upper_data = df['bb_upper'].dropna()
        bb_lower_data = df['bb_lower'].dropna()
        
        if not bb_upper_data.empty and not bb_lower_data.empty:
            # Upper band
            fig.add_trace(go.Scatter(
                x=df.loc[bb_upper_data.index, 'processing_time'],
                y=bb_upper_data,
                mode='lines',
                line_color='rgba(255,255,255,0.5)',
                name='BB Upper',
                line=dict(width=1)
            ))
            
            # Lower band
            fig.add_trace(go.Scatter(
                x=df.loc[bb_lower_data.index, 'processing_time'],
                y=bb_lower_data,
                mode='lines',
                line_color='rgba(255,255,255,0.5)',
                name='BB Lower',
                line=dict(width=1)
            ))

    fig.update_layout(
        title_text=f'Price Action & Technical Indicators', 
        template='plotly_dark',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Technical Indicators Row ---
    st.subheader("🔧 Technical Analysis")
    tech_cols = st.columns(4)
    
    with tech_cols[0]:
        display_technical_indicator("Momentum (10)", latest_data.get('momentum_10'), "{:.4f}")
        display_technical_indicator("Price Change %", latest_data.get('price_change_pct'), "{:.2f}%")
    
    with tech_cols[1]:
        display_technical_indicator("SMA (10)", latest_data.get('sma_10'), "${:.2f}")
        display_technical_indicator("SMA (20)", latest_data.get('sma_20'), "${:.2f}")
    
    with tech_cols[2]:
        display_technical_indicator("BB Upper", latest_data.get('bb_upper'), "${:.2f}")
        display_technical_indicator("BB Lower", latest_data.get('bb_lower'), "${:.2f}")
    
    with tech_cols[3]:
        display_technical_indicator("Volatility (20)", latest_data.get('volatility_20'), "{:.3f}")
        display_technical_indicator("Processing Time", latest_data.get('processing_time', 'N/A'), "{}")

    # --- ML Model Predictions ---
    st.subheader("🤖 Machine Learning Predictions")
    ml_models = ['XGBoost', 'RandomForest', 'AdaBoost', 'LogisticReg']
    available_ml = [model for model in ml_models if model in latest_data]
    
    if available_ml:
        ml_cols = st.columns(len(available_ml))
        for i, model in enumerate(available_ml):
            with ml_cols[i]:
                display_signal(model, latest_data[model])
    
    # --- Rule-Based Predictions ---
    st.subheader("📋 Rule-Based Signals")
    rule_models = ['RSI_Rule', 'MACD_Rule', 'Momentum_Rule', 'PriceChange_Rule', 'BB_Rule']
    available_rules = [model for model in rule_models if model in latest_data]
    
    if available_rules:
        rule_cols = st.columns(len(available_rules))
        for i, rule in enumerate(available_rules):
            with rule_cols[i]:
                display_signal(rule, latest_data[rule])
    
    # --- Recent Data Table ---
    st.subheader("📊 Recent Data")
    display_cols = ['processing_time', 'current_price', 'rsi_14', 'macd_line', 'volatility_20']
    available_display_cols = [col for col in display_cols if col in df.columns]
    
    # Add prediction columns if available
    prediction_cols = ['XGBoost', 'RandomForest', 'RSI_Rule', 'MACD_Rule']
    for col in prediction_cols:
        if col in df.columns:
            available_display_cols.append(col)
    
    if available_display_cols:
        # Format the dataframe for display
        display_df = df[available_display_cols].tail(10).copy()
        
        # Format numeric columns
        if 'current_price' in display_df.columns:
            display_df['current_price'] = display_df['current_price'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        if 'rsi_14' in display_df.columns:
            display_df['rsi_14'] = display_df['rsi_14'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        if 'macd_line' in display_df.columns:
            display_df['macd_line'] = display_df['macd_line'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "N/A")
        if 'volatility_20' in display_df.columns:
            display_df['volatility_20'] = display_df['volatility_20'].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "N/A")
        
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No data columns available for display")

else:
    st.info(f"Waiting for new data for symbol: {selected_symbol}...")
    st.info("Make sure your real_feature_factory.py and real_ml_consumer.py are running to populate the database.")