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

    # Initialize trend, trend_in_progress columns with all zeros
    df['trend'] = 0
    df['trend_in_progress'] = 0

    upper_band = 61.8
    lower_band = 38.2

    length = 22
    start_date = None
    mid_date = None
    finish_date = None

    # Find periods of trending market
    for i in range(1, len(df)):
        # 1. When the CI is above upper_band, the trend is 0
        if df['CI'].iloc[i] > upper_band:
            start_date, mid_date, finish_date = None, None, None
            df.loc[df.index[i], 'trend'] = 0
            df.loc[df.index[i], 'trend_in_progress'] = 0

        # + 2. When the CI is below upper_band and was above upper_band in the previous row, we mark a start_date and
        # set trend and trend_in_progress to 0.5
        elif df['CI'].iloc[i] < upper_band <= df['CI'].iloc[i - 1] and start_date is None:
            start_date = df.index[i]
            df.loc[df.index[i], 'trend'] = 0.5
            df.loc[df.index[i], 'trend_in_progress'] = 0.5

        # Options after point 2
        elif start_date is not None and mid_date is None:
            # Update trend and trend_in_progress columns for the current row
            df.loc[df.index[i], 'trend'] = 0.5
            df.loc[df.index[i], 'trend_in_progress'] = 0.5

            # Option 1: CI goes below lower_band
            if df['CI'].iloc[i] < lower_band and mid_date is None:
                mid_date = df.index[i]
                df.loc[start_date:mid_date, 'trend'] = 1
                # Leave 'trend_in_progress' as 0.5 for start_date to mid_date and set it to 0 from now
                df.loc[df.index[df.index.get_loc(mid_date):], 'trend_in_progress'] = 0

                # Loop through rows after mid_date to find when the CI goes back above lower_band
                for j in range(df.index.get_loc(mid_date) + 1, len(df)):
                    if df['CI'].iloc[j] > lower_band:
                        finish_date = df.index[j]
                        break
                    else:
                        df.loc[df.index[j], 'trend'] = 1
                        df.loc[df.index[j], 'trend_in_progress'] = 0

                # Set the trend and trend_in_progress to 0 from finish_date onwards
                if finish_date is not None:
                    df.loc[df.index[df.index.get_loc(finish_date):], 'trend'] = 0
                    df.loc[df.index[df.index.get_loc(finish_date):], 'trend_in_progress'] = 0

            # Option 2: CI goes above upper_band before going below lower_band
            elif df['CI'].iloc[i] > upper_band:
                mid_date, finish_date = df.index[i], df.index[i]
                df.loc[start_date:finish_date, 'trend'] = 0
                df.loc[df.index[df.index.get_loc(mid_date)]:, 'trend_in_progress'] = 0

            # Option 3: CI doesn't go below lower_band or above upper_band within 'length' rows
            elif i == df.index.get_loc(start_date) + length - 1 and mid_date is None:
                mid_date, finish_date = df.index[i], df.index[i]
                df.loc[start_date:mid_date, 'trend'] = 0
                df.loc[df.index[df.index.get_loc(mid_date)]:, 'trend_in_progress'] = 0

    # Save the results to a CSV file
    df.to_csv(f'processed_CI_data/{symbol}_CI.csv')
