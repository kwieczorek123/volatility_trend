import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

def read_data(file, pnl=False):
    df = pd.read_csv(file, index_col=0, parse_dates=True)
    if pnl:
        return df["volume"]
    print(f"Columns in {file}: {df.columns}")
    return df

def calculate_weights_close(price_data):
    last_month = price_data.iloc[-1]
    return last_month

def calculate_weights_volume(volume_data, close_data, lot_sizes):
    last_three_months = volume_data.iloc[-3:]
    average_notional_volume = (last_three_months * close_data.iloc[-3:] * lot_sizes).mean()
    return average_notional_volume

# Read input data
usdjpy_data = read_data('vol_trend_data/USDJPY_chop.csv')

# Convert USDJPY to JPYUSD
jpyusd_data = usdjpy_data.apply(lambda x: 1 / x)
jpyusd_data.to_csv('vol_trend_data/JPYUSD_chop.csv')

# Modify file_names list to use JPYUSD instead of USDJPY
file_names = ['vol_trend_data/EURUSD_chop.csv', 'vol_trend_data/GBPUSD_chop.csv', 'vol_trend_data/JPYUSD_chop.csv',
              'vol_trend_data/XAUUSD_chop.csv']
pnl_files = ['vol_trend_data/EURUSD_pnl.csv', 'vol_trend_data/GBPUSD_pnl.csv', 'vol_trend_data/USDJPY_pnl.csv',
             'vol_trend_data/XAUUSD_pnl.csv']

price_data = [read_data(file) for file in file_names]
volume_data = [read_data(file, pnl=True) for file in pnl_files]

lot_sizes = pd.Series([100000, 100000, 100000, 100], index=file_names)

# Calculate weights
weights_close = [calculate_weights_close(df) for df in price_data]
weights_volume = [calculate_weights_volume(vol, price, lot_sizes[i]) for i, (vol, price) in
                  enumerate(zip(volume_data, price_data))]

normalized_weights = [weight_close * weight_volume for weight_close, weight_volume in
                      zip(weights_close, weights_volume)]

top4_ohlc = sum([price.div(norm_weight.iloc[0]) for price, norm_weight in zip(price_data, normalized_weights)]).dropna()
top4_ohlc.index.name = "date"

# Save the OHLC data to a new CSV file 'top4.csv'
top4_ohlc.to_csv("top4.csv")
