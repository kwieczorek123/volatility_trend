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


    def check_filter(df, start_idx, threshold, time_filter):
        end_idx = min(start_idx + time_filter + 1, len(df))
        below_threshold = df.iloc[start_idx:end_idx]['CI'] < threshold
        if below_threshold.any():
            return df.index[df.index.get_loc(df.iloc[start_idx].name) + below_threshold.idxmax()]
        return None


    def update_trend(df, start_date, mid_date, finish_date):
        if finish_date is not None:
            df.loc[mid_date:finish_date, 'trend'] = 1
            df.loc[finish_date:, ['trend', 'trend_in_progress']] = 0
        else:
            df.loc[start_date:mid_date, 'trend'] = 1
            df.loc[mid_date:, 'trend_in_progress'] = 0
        return df


    def update_trend_reloop(df, mid_date, finish_date):
        df.loc[mid_date:finish_date, 'trend'] = 1
        df.loc[finish_date:, ['trend', 'trend_in_progress']] = 0
        return df


    def apply_trends(df, start_date, mid_date, finish_date):
        if start_date is not None:
            df = update_trend(df, start_date, mid_date, finish_date)
            if finish_date is not None:
                df = update_trend_reloop(df, mid_date, finish_date)
        return df


    def update_trend_for_subsequent_rows(df, start_date, mid_date, finish_date, reloop_lenght):
        while mid_date is not None and finish_date is not None:
            mid_date, finish_date = None, None
            for i in range(df.index.get_loc(start_date) + 1,
                           min(df.index.get_loc(start_date) + reloop_lenght + 1, len(df))):
                if df.loc[df.index[i], 'CI'] < lower_band:
                    mid_date, finish_date = df.index[i], df.index[i]
                    df.loc[df.index[i], 'trend'] = 1
                    df.loc[df.index[i], 'trend_in_progress'] = 0

                    for j in range(df.index.get_loc(mid_date) + 1, len(df)):
                        if df.loc[df.index[j], 'CI'] > lower_band:
                            finish_date = df.index[j]
                            df = update_trend_reloop(df, mid_date, finish_date)
                            break
                        else:
                            df.loc[df.index[j], 'trend'] = 1
                            df.loc[df.index[j], 'trend_in_progress'] = 0
            start_date = finish_date
        return df


    # Initialize trend, trend_in_progress columns with all zeros
    df['trend'] = 0
    df['trend_in_progress'] = 0

    # Define constants
    upper_band = 57
    lower_band = 40
    length = 20
    reloop_lenght = 5
    start_date = None
    mid_date = None
    finish_date = None
    time_filter = 3
    first_treshold_filter = 50
    second_time_filter = 3
    second_treshold_filter = 45

    # Iterate through the dataframe
    for i in range(len(df)):
        if df.loc[df.index[i], 'CI'] > lower_band:
            if start_date is None:
                start_date = df.index[i]
            elif mid_date is None:
                mid_date = df.index[i]
                finish_date = check_filter(df, i, lower_band, time_filter)
                df = apply_trends(df, start_date, mid_date, finish_date)
                if finish_date is not None:
                    reloop_length = 5
                    df = update_trend_for_subsequent_rows(df, start_date, mid_date, finish_date, reloop_length)
                    start_date, mid_date, finish_date = None, None, None
        elif mid_date is not None:
            finish_date = df.index[i]
            df = update_trend(df, start_date, mid_date, finish_date)
            start_date, mid_date, finish_date = None, None, None

    # Drop unnecessary columns
    df.drop(columns=['TR', 'ATR'], inplace=True)

    # Save the results to a CSV file
    df.to_csv(f'processed_CI_data/{symbol}_CI.csv')
