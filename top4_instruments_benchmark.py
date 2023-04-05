import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt


# Define a function to read in the price OHLC_data from CSV files
def read_data(file, pnl=False):
    # Read in the CSV file and set the index to the datetime column
    df = pd.read_csv(file, index_col=0, parse_dates=True)
    # Remove duplicate rows with the same index
    df = df.loc[~df.index.duplicated(keep='last')]
    # Sort the rows by the index (datetime)
    df = df.sort_index()

    if pnl:
        # If the 'pnl' flag is True, return only the 'volume' column (for calculating volume weights later)
        return df["volume"]

    print(f"Columns in {file}: {df.columns}")

    # Check if 'trend' column exists, then create a copy of the DataFrame without it
    if 'trend' in df.columns:
        df_no_trend = df.drop(columns=['trend'])
    else:
        df_no_trend = df.copy()

    # Return the DataFrame with the 'trend' column dropped (if it exists)
    return df_no_trend


# Define a function to calculate the closing prices for each month (used for calculating volume weights)
def calculate_weights_close(price_data):
    # Create a dictionary to store the last day's closing price for each month
    last_days_of_months = {}
    for month_end in price_data.resample("M").last().index:
        # Find the index location of the last day of the month
        loc_close_data = price_data.index.get_indexer([month_end], method='pad')[0]
        # Get the closing price for that day
        last_day = price_data.loc[price_data.index[loc_close_data], 'close']
        # Add the closing price to the dictionary
        last_days_of_months[month_end] = last_day

    # Return the dictionary of last day's closing prices for each month
    return last_days_of_months


# Define a function to calculate the volume weights for each month
def calculate_weights_volume(volume_data, close_data, lot_size):
    # Get the last day's closing prices for each month
    last_close_dates_prices = calculate_weights_close(close_data)
    # Create a dictionary to store the notional volume for each month-end
    volume_month_end = {}

    for date, close_price in last_close_dates_prices.items():
        # Create a mask to filter the volume OHLC_data up to the current month-end
        mask = (volume_data.index <= date)
        filtered_volume_data = volume_data[mask]
        # Get the last 3 month-ends' volumes
        last_3_month_ends = filtered_volume_data.loc[filtered_volume_data.index.is_month_end].iloc[-3:]

        if lot_size in close_data.columns:  # XAUUSD case
            # Calculate the notional volume for XAUUSD
            notional_volume = (last_3_month_ends * lot_size * close_price).sum()
        elif 'USDJPY' in close_data.columns:  # USDJPY case
            # Calculate the notional volume for USDJPY
            notional_volume = (last_3_month_ends * lot_size).sum()
        else:  # EURUSD and GBPUSD case
            # Calculate the notional volume for EURUSD and GBPUSD
            notional_volume = (last_3_month_ends * lot_size / close_price).sum()

        # Add the notional volume to the dictionary
        volume_month_end[date] = notional_volume

    return volume_month_end


# Read USDJPY chop OHLC_data and calculate inverse
usdjpy_data = read_data('results/trend_volatility_results/processed_CI_data/USDJPY_CI.csv')
jpyusd_data = usdjpy_data.apply(lambda x: 1 / x)
jpyusd_data.to_csv('results/top4_benchmark_results/JPYUSD_CI.csv')

# Read price and volume OHLC_data for top 4 instruments
file_names = ['results/trend_volatility_results/processed_CI_data/EURUSD_CI.csv', 'results/trend_volatility_results'
                                                                                  '/processed_CI_data/GBPUSD_CI.csv',
              'results/top4_benchmark_results/JPYUSD_CI.csv',
              'results/trend_volatility_results/processed_CI_data/XAUUSD_CI.csv']
pnl_files = ['Input data/trend_volatility_input_data/EURUSD_pnl.csv', 'Input data/trend_volatility_input_data'
                                                                      '/GBPUSD_pnl.csv',
             'Input data/trend_volatility_input_data/USDJPY_pnl.csv',
             'Input data/trend_volatility_input_data/XAUUSD_pnl.csv']
price_data = [read_data(file) for file in file_names]
volume_data = [read_data(file, pnl=True) for file in pnl_files]

# Define lot sizes for each instrument
lot_sizes = pd.Series([100000, 100000, 100000, 100], index=file_names)

# Calculate weights based on last day of each month for price and volume OHLC_data
weights_close = [calculate_weights_close(df) for df in price_data]
print(weights_close)

weights_volume = [calculate_weights_volume(vol, price, lot_sizes[i]) for i, (vol, price) in
                  enumerate(zip(volume_data, price_data))]
print(weights_volume)

# Normalize the weights using a small constant
normalized_constant = 1e-10
normalized_weights = [
    {
        key: weight_close[key] / (weight_volume[key] + normalized_constant)
        for key in weight_close
    }
    for weight_close, weight_volume in zip(weights_close, weights_volume)
]

print(normalized_weights)

# Calculate the top 4 OHLC OHLC_data based on normalized weights
top4_ohlc = sum([price.div(next(iter(norm_weight.values()))) for price, norm_weight in
                 zip(price_data, normalized_weights)]).dropna()

# Rename index to 'date'
top4_ohlc.index.name = "date"

# Scale the OHLC_data using the normalization constant
top4_ohlc *= 1 / normalized_constant

# Save the OHLC OHLC_data to a new CSV file 'top4.csv'
top4_ohlc.to_csv("results/top4_benchmark_results/top4.csv")


# Define function to plot candlestick chart with volume
def plot_candlestick_with_volume(df, title="", warn_too_much_data=None):
    # Create figure with 2 subplots (price and volume)
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
    fig.subplots_adjust(hspace=0)

    # Plot the candlestick chart in the top subplot
    mpf.plot(df, type='candle', ax=axes[0], warn_too_much_data=warn_too_much_data)
    axes[0].set_title(title)
    axes[0].set_ylabel('Price')

    # Plot the volume in the bottom subplot
    volume = df['volume'].fillna(0)
    volume.plot(kind='bar', ax=axes[1], color='blue', alpha=0.7)
    axes[1].set_ylabel('Volume')

    # Show the plot
    plt.show()


# Merge the volume OHLC_data into the top4_ohlc DataFrame
merged_volume_data = sum(
    [vol.div(next(iter(norm_weight.values()))) for vol, norm_weight in zip(volume_data, normalized_weights)]).dropna()
top4_ohlc['volume'] = merged_volume_data


# Define function to resample OHLC_data to weekly OHLC
def resample_weekly(df):
    ohlc = df.resample('W').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})
    volume = df['volume'].resample('W').sum()
    ohlc['volume'] = volume
    return ohlc


# Resample the OHLC_data to weekly OHLC
top4_ohlc_weekly = resample_weekly(top4_ohlc)

# Plot the candlestick chart with combined notional volume
plot_candlestick_with_volume(top4_ohlc_weekly, title="Top 4 Instrument Weekly OHLC & Volume", warn_too_much_data=10000)

