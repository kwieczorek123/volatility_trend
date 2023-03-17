import pandas as pd
import numpy as np
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


def read_data(file, pnl=False):
    df = pd.read_csv(file, index_col=0, parse_dates=True)
    df = df.loc[~df.index.duplicated(keep='last')]
    df = df.sort_index()
    if pnl:
        return df["volume"]
    print(f"Columns in {file}: {df.columns}")

    # Check if 'trend' column exists, then create a copy of the DataFrame without it
    if 'trend' in df.columns:
        df_no_trend = df.drop(columns=['trend'])
    else:
        df_no_trend = df.copy()

    return df_no_trend


def calculate_weights_close(price_data):
    last_days_of_months = {}
    # Removed the line to drop the 'trend' column, as it's already handled in read_data
    for month_end in price_data.resample("M").last().index:
        loc_close_data = price_data.index.get_indexer([month_end], method='pad')[0]
        last_day = price_data.loc[price_data.index[loc_close_data], 'close']
        last_days_of_months[month_end] = last_day
    return last_days_of_months


def calculate_weights_volume(volume_data, close_data, lot_size):
    last_close_dates_prices = calculate_weights_close(close_data)
    volume_month_end = {}

    for date, close_price in last_close_dates_prices.items():
        mask = (volume_data.index <= date)
        filtered_volume_data = volume_data[mask]
        last_3_month_ends = filtered_volume_data.loc[filtered_volume_data.index.is_month_end].iloc[-3:]

        if lot_size in close_data.columns:  # XAUUSD case
            notional_volume = (last_3_month_ends * lot_size * close_price).sum()
        elif 'USDJPY' in close_data.columns:  # USDJPY case
            notional_volume = (last_3_month_ends * lot_size).sum()
        else:  # EURUSD and GBPUSD case
            notional_volume = (last_3_month_ends * lot_size / close_price).sum()

        volume_month_end[date] = notional_volume

    return volume_month_end


usdjpy_data = read_data('vol_trend_data/USDJPY_chop.csv')

jpyusd_data = usdjpy_data.apply(lambda x: 1 / x)
jpyusd_data.to_csv('vol_trend_data/JPYUSD_chop.csv')

file_names = ['vol_trend_data/EURUSD_chop.csv', 'vol_trend_data/GBPUSD_chop.csv', 'vol_trend_data/JPYUSD_chop.csv',
              'vol_trend_data/XAUUSD_chop.csv']
pnl_files = ['vol_trend_data/EURUSD_pnl.csv', 'vol_trend_data/GBPUSD_pnl.csv', 'vol_trend_data/USDJPY_pnl.csv',
             'vol_trend_data/XAUUSD_pnl.csv']

price_data = [read_data(file) for file in file_names]
volume_data = [read_data(file, pnl=True) for file in pnl_files]

lot_sizes = pd.Series([100000, 100000, 100000, 100], index=file_names)

weights_close = [calculate_weights_close(df) for df in price_data]
print(weights_close)

weights_volume = [calculate_weights_volume(vol, price, lot_sizes[i]) for i, (vol, price) in
                  enumerate(zip(volume_data, price_data))]
print(weights_volume)

normalized_constant = 1e-10

normalized_weights = [
    {
        key: weight_close[key] / (weight_volume[key] + normalized_constant)
        for key in weight_close
    }
    for weight_close, weight_volume in zip(weights_close, weights_volume)
]

print(normalized_weights)

top4_ohlc = sum([price.div(next(iter(norm_weight.values()))) for price, norm_weight in
                 zip(price_data, normalized_weights)]).dropna()

top4_ohlc.index.name = "date"

top4_ohlc *= 1 / normalized_constant

# Save the OHLC data to a new CSV file 'top4.csv'
top4_ohlc.to_csv("top4.csv")


def plot_candlestick_with_volume(df, title="", warn_too_much_data=None):
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
    fig.subplots_adjust(hspace=0)

    mpf.plot(df, type='candle', ax=axes[0], warn_too_much_data=warn_too_much_data)
    axes[0].set_title(title)
    axes[0].set_ylabel('Price')

    volume = df['volume'].fillna(0)
    volume.plot(kind='bar', ax=axes[1], color='blue', alpha=0.7)
    axes[1].set_ylabel('Volume')

    plt.show()


# Merge the volume data into the top4_ohlc DataFrame
merged_volume_data = sum(
    [vol.div(next(iter(norm_weight.values()))) for vol, norm_weight in zip(volume_data, normalized_weights)]).dropna()
top4_ohlc['volume'] = merged_volume_data

# Plot the candlestick chart with combined notional volume
plot_candlestick_with_volume(top4_ohlc, title="Top 4 Instrument OHLC & Volume", warn_too_much_data=10000)
