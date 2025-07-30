import json
import time
from collections import defaultdict, deque
from datetime import datetime
import math
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import numpy as np

class TechnicalIndicatorCalculator:
    def __init__(self):
        # State storage for each symbol
        self.price_history = defaultdict(lambda: deque(maxlen=100))  # Keep last 100 prices
        self.volume_history = defaultdict(lambda: deque(maxlen=100))
        self.timestamp_history = defaultdict(lambda: deque(maxlen=100))
        
    def calculate_sma(self, prices, window):
        """Calculate Simple Moving Average"""
        if len(prices) < window:
            return None
        return sum(list(prices)[-window:]) / window
    
    def calculate_ema(self, prices, window):
        """Calculate Exponential Moving Average"""
        if len(prices) < window:
            return None
        prices_list = list(prices)
        alpha = 2.0 / (window + 1)
        ema = prices_list[0]
        for price in prices_list[1:]:
            ema = alpha * price + (1 - alpha) * ema
        return ema
    
    def calculate_rsi(self, prices, window=14):
        """Calculate Relative Strength Index"""
        if len(prices) < window + 1:
            return None
        
        prices_list = list(prices)
        deltas = [prices_list[i] - prices_list[i-1] for i in range(1, len(prices_list))]
        
        gains = [delta if delta > 0 else 0 for delta in deltas]
        losses = [-delta if delta < 0 else 0 for delta in deltas]
        
        avg_gain = sum(gains[-window:]) / window
        avg_loss = sum(losses[-window:]) / window
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        """Calculate MACD"""
        if len(prices) < slow:
            return None, None, None
        
        ema_fast = self.calculate_ema(prices, fast)
        ema_slow = self.calculate_ema(prices, slow)
        
        if ema_fast is None or ema_slow is None:
            return None, None, None
        
        macd_line = ema_fast - ema_slow
        
        # For signal line, we need MACD history
        macd_history = deque(maxlen=50)
        for i in range(slow, len(prices)):
            ema_fast_temp = self.calculate_ema(deque(list(prices)[:i+1]), fast)
            ema_slow_temp = self.calculate_ema(deque(list(prices)[:i+1]), slow)
            macd_history.append(ema_fast_temp - ema_slow_temp)
        
        if len(macd_history) < signal:
            signal_line = None
        else:
            signal_line = self.calculate_ema(macd_history, signal)
        
        histogram = macd_line - signal_line if signal_line is not None else None
        
        return macd_line, signal_line, histogram
    
    def calculate_bollinger_bands(self, prices, window=20, num_std=2):
        """Calculate Bollinger Bands"""
        if len(prices) < window:
            return None, None, None
        
        prices_list = list(prices)[-window:]
        sma = sum(prices_list) / window
        
        variance = sum((price - sma) ** 2 for price in prices_list) / window
        std_dev = math.sqrt(variance)
        
        upper_band = sma + (num_std * std_dev)
        lower_band = sma - (num_std * std_dev)
        
        return upper_band, sma, lower_band
    
    def calculate_volatility(self, prices, window=20):
        """Calculate Volatility"""
        if len(prices) < window:
            return None
        
        prices_list = list(prices)[-window:]
        returns = []
        
        for i in range(1, len(prices_list)):
            if prices_list[i-1] != 0:
                ret = (prices_list[i] - prices_list[i-1]) / prices_list[i-1]
                returns.append(ret)
        
        if len(returns) == 0:
            return None
        
        return np.std(returns) * math.sqrt(252)  # Annualized volatility
    
    def calculate_momentum(self, prices, window=10):
        """Calculate Momentum"""
        if len(prices) < window:
            return None
        
        prices_list = list(prices)
        current_price = prices_list[-1]
        past_price = prices_list[-window]
        
        return (current_price - past_price) / past_price
    
    def generate_features(self, symbol, price, timestamp):
        """Generate all technical indicators for a symbol"""
        # Add to history
        self.price_history[symbol].append(price)
        self.timestamp_history[symbol].append(timestamp)
        
        prices = self.price_history[symbol]
        
        if len(prices) < 5:  # Need minimum data
            return None
        
        features = {
            'symbol': symbol,
            'current_price': price,
            'timestamp': timestamp,
            'processing_time': datetime.now().isoformat()
        }
        
        # Moving Averages
        features['sma_5'] = self.calculate_sma(prices, 5)
        features['sma_10'] = self.calculate_sma(prices, 10)
        features['sma_20'] = self.calculate_sma(prices, 20)
        features['ema_12'] = self.calculate_ema(prices, 12)
        features['ema_26'] = self.calculate_ema(prices, 26)
        
        # RSI
        features['rsi_14'] = self.calculate_rsi(prices, 14)
        
        # MACD
        macd_line, signal_line, histogram = self.calculate_macd(prices)
        features['macd_line'] = macd_line
        features['macd_signal'] = signal_line
        features['macd_histogram'] = histogram
        
        # Bollinger Bands
        bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(prices)
        features['bb_upper'] = bb_upper
        features['bb_middle'] = bb_middle
        features['bb_lower'] = bb_lower
        
        # Volatility
        features['volatility_20'] = self.calculate_volatility(prices, 20)
        
        # Momentum
        features['momentum_10'] = self.calculate_momentum(prices, 10)
        
        # Price changes
        if len(prices) >= 2:
            features['price_change'] = price - list(prices)[-2]
            features['price_change_pct'] = (price - list(prices)[-2]) / list(prices)[-2] * 100
        
        # Lagged prices
        if len(prices) >= 2:
            features['lag_1'] = list(prices)[-2]
        if len(prices) >= 3:
            features['lag_2'] = list(prices)[-3]
        if len(prices) >= 6:
            features['lag_5'] = list(prices)[-6]
        
        return features

def main():
    # Initialize
    calculator = TechnicalIndicatorCalculator()
    
    # Kafka setup
    consumer = KafkaConsumer(
        'futures-ticks',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print("🚀 REAL TECHNICAL INDICATOR FEATURE FACTORY STARTED")
    print("Generating: SMA, EMA, RSI, MACD, Bollinger Bands, Volatility, Momentum")
    print("=" * 60)
    
    try:
        for message in consumer:
            data = message.value
            
            # Extract data
            symbol = data['symbol']
            price = float(data['price'])
            timestamp = data.get('timestamp', datetime.now().isoformat())
            
            # Generate features
            features = calculator.generate_features(symbol, price, timestamp)
            
            if features:
                # Send to Kafka
                producer.send('real-features-ticks', features)
                
                # Print sample output
                rsi_val = features.get('rsi_14')
                macd_val = features.get('macd_line')
                vol_val = features.get('volatility_20')
                
                rsi_str = f"{rsi_val:.2f}" if rsi_val is not None else "N/A"
                macd_str = f"{macd_val:.4f}" if macd_val is not None else "N/A"
                vol_str = f"{vol_val:.3f}" if vol_val is not None else "N/A"
                
                print(f"📊 {symbol}: Price=${price:.2f} | RSI={rsi_str} | MACD={macd_str} | Vol={vol_str}")
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping feature factory...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main() 