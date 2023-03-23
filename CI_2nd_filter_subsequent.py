import pandas as pd
import numpy as np

list_of_symbols = ['EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD']

for symbol in list_of_symbols:

    # Read in the data
    df = pd.read_csv(f"data/{symbol}/{symbol}1440.csv",
                     names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
    # Set the date as the index
    df.set_index('date', inplace=True)
    # Drop the time and volume columns
    df.drop(columns=['time', 'volume'], inplace=True)

    # Calculate the true range (TR)
    df['TR'] = np.maximum(np.maximum(df['high'] - df['low'], np.abs(df['high'] - df['close'].shift())),
                          np.abs(df['low'] - df['close'].shift()))

    # Calculate the average true range (ATR)
    n = 14
    df['ATR'] = df['TR'].rolling(n).mean()

    # Calculate the choppiness index (CI)
    numerator = np.log10(df['ATR'].rolling(n).sum() / (df['high'].rolling(n).max() - df['low'].rolling(n).min()))
    denominator = np.log10(n)
    df['CI'] = 100 * numerator / denominator

    # Define constants
    upper_band = 57
    lower_band = 40
    length = 20
    reloop_length = 10
    first_treshold_filter = 5
    second_treshold_filter = 10
    time_filter = 15
    second_time_filter = 10

    start_date = None
    mid_date = None
    finish_date = None

    # Initialize 'trend' and 'trend_in_progress' columns
    df['trend'] = 0
    df['trend_in_progress'] = 0

    # Main loop
    i = 0
    while i < len(df):
        # If CI is above upper_band
        if df['CI'].iloc[i] > upper_band:
            start_date, mid_date, finish_date = None, None, None
            df.loc[df.index[i], 'trend'] = 0
            df.loc[df.index[i], 'trend_in_progress'] = 0
            i += 1
        else:
            # If CI is below upper_band and no start_date is defined
            if start_date is None:
                start_date = df.index[i]
                df.loc[start_date, 'trend'] = 0.5
                df.loc[start_date, 'trend_in_progress'] = 0.5

                # FIRST LOOP
                for j in range(i + 1, min(i + length + 1, len(df))):
                    # Check if CI goes below the lower_band within the length period
                    if df['CI'].iloc[j] < lower_band:
                        mid_date = df.index[j]
                        df.loc[start_date:mid_date, 'trend'] = 1
                        df.loc[mid_date:, 'trend_in_progress'] = 0
                        i = j
                        break
                    # Check if CI goes above the upper_band before going below the lower_band
                    elif df['CI'].iloc[j] > upper_band:
                        finish_date = df.index[j]
                        df.loc[start_date:finish_date, 'trend'] = 0
                        df.loc[finish_date:, 'trend_in_progress'] = 0
                        start_date, mid_date = None, None
                        i = j
                        break
                else:
                    # If neither condition met within the length period, set finish_date
                    finish_date = df.index[min(i + length, len(df) - 1)]
                    df.loc[start_date:finish_date, 'trend'] = 0
                    df.loc[finish_date:, 'trend_in_progress'] = 0
                    start_date, mid_date = None, None

    # Drop unnecessary columns
    df.drop(columns=['TR', 'ATR'], inplace=True)

    # Save the results to a CSV file
    df.to_csv(f'processed_CI_data/{symbol}_CI.csv')