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


    def update_trend(df, start_date, mid_date, finish_date):
        """
        It sets the 'trend' column of rows with dates between start_date and mid_date (inclusive) to 1. It sets the
        'trend_in_progress' column of rows with dates after mid_date to 0. If finish_date is not None, it sets the
        'trend' and 'trend_in_progress' columns of rows with dates after finish_date to 0. Finally, the function
        returns the updated DataFrame.
        """
        df.loc[start_date:mid_date, 'trend'] = 1
        df.loc[df.index[df.index.get_loc(mid_date):], 'trend_in_progress'] = 0
        if finish_date is not None:
            df.loc[df.index[df.index.get_loc(finish_date):], 'trend'] = 0
            df.loc[df.index[df.index.get_loc(finish_date):], 'trend_in_progress'] = 0
        return df


    def check_below_first_treshold_filter(df, i, time_filter, first_treshold_filter, second_time_filter,
                                          second_treshold_filter):
        # Check if CI goes below the first_treshold_filter within the time_filter period
        below_first_treshold_filter = False
        first_filter_date = None
        for j in range(i + 1, min(i + time_filter + 1, len(df))):
            if df['CI'].iloc[j] < first_treshold_filter:
                below_first_treshold_filter = True
                first_filter_date = df.index[j]
                break

        if not below_first_treshold_filter:
            # If CI doesn't go below the first_treshold_filter in the time_filter period
            finish_date = df.index[min(i + time_filter, len(df) - 1)]
            if j < i + time_filter:
                # The loop did not go through time_filter rows
                # Leave the trend column as 0.5 for the remaining rows
                df.loc[df.index[i:], 'trend'] = 0.5
            else:
                # The loop went through time_filter rows
                df.loc[df.index[i]:finish_date, 'trend'] = 0
                df.loc[df.index[df.index.get_loc(finish_date)], 'trend_in_progress'] = 0
        else:
            # If CI goes below the first_treshold_filter in the time_filter period
            for j in range(i + 1, df.index.get_loc(first_filter_date) + 1):
                df.loc[df.index[j], 'trend'] = 0.5
                df.loc[df.index[j], 'trend_in_progress'] = 0.5

            # Check if CI goes below the second_treshold_filter within the second_time_filter period
            below_second_treshold_filter = False
            second_filter_date = None
            for j in range(df.index.get_loc(first_filter_date) + 1,
                           min(df.index.get_loc(first_filter_date) + second_time_filter + 1, len(df))):
                if df['CI'].iloc[j] < second_treshold_filter:
                    below_second_treshold_filter = True
                    second_filter_date = df.index[j]
                    break

            if not below_second_treshold_filter:
                # If CI doesn't go below the second_treshold_filter in the second_time_filter period
                finish_date = df.index[min(df.index.get_loc(first_filter_date) + second_time_filter, len(df) - 1)]
                if j < df.index.get_loc(first_filter_date) + second_time_filter:
                    # The loop did not go through second_time_filter rows
                    # Leave the trend column as 0.5 for the remaining rows
                    df.loc[df.index[df.index.get_loc(first_filter_date):], 'trend'] = 0.5
                else:
                    # The loop went through second_time_filter rows
                    df.loc[df.index[df.index.get_loc(first_filter_date)]:finish_date, 'trend'] = 0
                    df.loc[df.index[df.index.get_loc(finish_date)], 'trend_in_progress'] = 0
            else:
                # If CI goes below the second_treshold_filter in the second_time_filter period
                for j in range(df.index.get_loc(first_filter_date) + 1, df.index.get_loc(second_filter_date) + 1):
                    df.loc[df.index[j], 'trend'] = 0.5

            # Change the values in trend column to 0, when trend didn't materialize
            last_zero_index = df[df['trend_in_progress'] == 0].index[-1]
            df.loc[((df.index < last_zero_index) | (df.index > last_zero_index)) & (df['trend'] == 0.5), 'trend'] = 0

        return df


    # Initialize trend, trend_in_progress columns with all zeros
    df['trend'] = 0
    df['trend_in_progress'] = 0

    # Define constants
    upper_band = 57
    lower_band = 40
    length = 20
    reloop_lenght = 15
    start_date = None
    mid_date = None
    finish_date = None
    time_filter = 3
    first_treshold_filter = 50
    second_time_filter = 3
    second_treshold_filter = 45

    # Find periods of trending market
    for i in range(1, len(df)):
        # 1. When the CI is above upper_band, the trend is 0
        if df['CI'].iloc[i] > upper_band:
            start_date, mid_date, finish_date = None, None, None
            df.loc[df.index[i], 'trend'] = 0
            df.loc[df.index[i], 'trend_in_progress'] = 0

        # 2. When the CI is below upper_band and was above upper_band in the previous row, we mark a start_date and
        # set trend and trend_in_progress to 0.5
        elif df['CI'].iloc[i] < upper_band <= df['CI'].iloc[i - 1] and start_date is None:
            start_date = df.index[i]
            df.loc[df.index[i], 'trend'] = 0.5
            df.loc[df.index[i], 'trend_in_progress'] = 0.5

            # Check if CI goes below the first_treshold_filter within the time_filter period
            below_first_treshold_filter = False
            first_filter_date = None
            for j in range(i + 1, min(i + time_filter + 1,
                                      len(df))):  # potential issue with len(df) when its shorter than time filer
                if df['CI'].iloc[j] < first_treshold_filter:
                    below_first_treshold_filter = True
                    first_filter_date = df.index[j]
                    break

            if not below_first_treshold_filter:
                # If CI doesn't go below the first_treshold_filter in the time_filter period
                mid_date = finish_date = first_filter_date

                # Mark trend as 0 and leave trend_in_progress as 0.5 from start_date to finish_date
                df.loc[start_date:finish_date, 'trend'] = 0
                df.loc[start_date:finish_date, 'trend_in_progress'] = 0.5
            else:
                # If CI goes below the first_treshold_filter in the time_filter period
                # Check if CI goes below the second_treshold_filter within the second_time_filter period
                below_second_treshold_filter = False
                second_filter_date = None
                for j in range(df.index.get_loc(first_filter_date) + 1,
                               min(df.index.get_loc(first_filter_date) + second_time_filter + 1, len(df))):  # same
                    if df['CI'].iloc[j] < second_treshold_filter:
                        below_second_treshold_filter = True
                        second_filter_date = df.index[j]
                        break

                if not below_second_treshold_filter:
                    # If CI doesn't go below the second_treshold_filter in the second_time_filter period
                    mid_date = finish_date = first_filter_date

                    # Mark trend as 0 and leave trend_in_progress as 0.5 from start_date to finish_date
                    df.loc[start_date:finish_date, 'trend'] = 0
                    df.loc[start_date:finish_date, 'trend_in_progress'] = 0.5
                else:
                    # If CI goes below the second_treshold_filter in the second_time_filter period
                    # Continue looping for CI to go below lower_band
                    df.loc[start_date:second_filter_date, 'trend'] = 0.5
                    df.loc[start_date:second_filter_date, 'trend_in_progress'] = 0.5

        # Options after point 2
        elif start_date is not None and mid_date is None:
            # Update trend and trend_in_progress columns for the current row
            df.loc[df.index[i], 'trend'] = 0.5
            df.loc[df.index[i], 'trend_in_progress'] = 0.5

            # Option 1: CI goes below lower_band
            if df['CI'].iloc[i] < lower_band and mid_date is None:
                mid_date = df.index[i]
                df = update_trend(df, start_date, mid_date, None)

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
                    df = update_trend(df, start_date, mid_date, finish_date)

                    # Scan for the next rows after the finish date
                    for k in range(df.index.get_loc(finish_date) + 1,
                                   min(df.index.get_loc(finish_date) + reloop_lenght + 1, len(df))):
                        if df['CI'].iloc[k] < lower_band:
                            start_date = mid_date = df.index[k]
                            df = update_trend(df, start_date, mid_date, None)

                            # Loop through rows after new_mid_date to find when the CI goes back above lower_band
                            for l in range(df.index.get_loc(mid_date) + 1, len(df)):
                                if df['CI'].iloc[l] > lower_band:
                                    finish_date = df.index[l]
                                    df = update_trend(df, start_date, mid_date, finish_date)
                                    break
                                else:
                                    df.loc[df.index[l], 'trend'] = 1
                                    df.loc[df.index[l], 'trend_in_progress'] = 0

            # Option 2: CI goes above upper_band before going below lower_band
            elif df['CI'].iloc[i] > upper_band:
                mid_date, finish_date = df.index[i], df.index[i]
                df.loc[start_date:finish_date, 'trend'] = 0
                df.loc[df.index[df.index.get_loc(finish_date)]:, 'trend_in_progress'] = 0

            # Option 3: CI doesn't go below lower_band or above upper_band within 'length' rows
            elif i == df.index.get_loc(start_date) + length - 1 and mid_date is None:
                mid_date, finish_date = df.index[i], df.index[i]
                df.loc[start_date:finish_date, 'trend'] = 0
                df.loc[df.index[df.index.get_loc(finish_date)]:, 'trend_in_progress'] = 0

    # This part ensures that any trends that might have been missed or continued after the main loop has completed are
    # properly labeled
    confirmed_trend_start = None
    for i in range(len(df)):
        # If trend is 1 and trend_in_progress is 0, we have a confirmed trend
        if df['trend'].iloc[i] == 1 and df['trend_in_progress'].iloc[i] == 0:
            if confirmed_trend_start is None:
                confirmed_trend_start = df.index[i]

        # If there is a finished trend and CI goes above 38.2, check the next reloop_lenght periods
        elif confirmed_trend_start is not None and df['CI'].iloc[i] > lower_band:
            trend_continued = False
            for j in range(i + 1, min(i + reloop_lenght + 1, len(df))):
                if df['CI'].iloc[j] < lower_band:
                    trend_continued = True
                    break

            if trend_continued:
                # Mark dates as trend=1 and continue until CI goes above 38.2
                while i < len(df) and df['CI'].iloc[i] < lower_band:
                    df.loc[df.index[i], 'trend'] = 1
                    i += 1
            else:
                confirmed_trend_start = None

    # Change the values in trend column to 0, when trend didn't materialize
    last_zero_index = df[df['trend_in_progress'] == 0].index[-1]
    df.loc[((df.index < last_zero_index) | (df.index > last_zero_index)) & (df['trend'] == 0.5), 'trend'] = 0

    # Drop unnecessary columns
    df.drop(columns=['TR', 'ATR'], inplace=True)

    # Save the results to a CSV file
    df.to_csv(f'processed_CI_data/{symbol}_CI.csv')
