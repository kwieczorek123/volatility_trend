import pandas as pd
import numpy as np

# Read in the data
df = pd.read_csv("data/USDJPY/USDJPY1440.csv", names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
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

# Initialize trend column with all zeros
df['trend'] = 0

lenght = 22
start_date = None
mid_date = None

# Find periods of trending market
for i in range(1, len(df)):
    # If current value of CI is less than 61.8 and previous value is greater than or equal to 61.8,
    # then we have entered a trending period. Set start_date and mid_date to current index and None respectively.
    if df['CI'].iloc[i] < 61.8 <= df['CI'].iloc[i - 1]:
        start_date = df.index[i]
        mid_date = None
    # If current value of CI is less than 38.2, and we have a start_date (i.e. we have entered a trending period),
    # but have not yet found a mid_date, search for a mid_date within the next 'lenght' number of rows.
    elif df['CI'].iloc[i] < 38.2 and start_date is not None and mid_date is None:
        for j in range(i + 1, min(i + lenght + 1, len(df))):
            if df['CI'].iloc[j] > 38.2:
                mid_date = df.index[j]
                break
        # If a mid_date is found, search for an end_date within the next 'lenght' number of rows.
        # If an end_date is found, set the 'trend' column to 1 for all dates between start_date and end_date,
        # and reset start_date and mid_date to None to indicate the end of the trending period.
        if mid_date is not None:
            for j in range(j + 1, min(j + lenght + 1, len(df))):
                if df['CI'].iloc[j] < 61.8:
                    end_date = df.index[j]
                    df.loc[start_date:end_date, 'trend'] = 1
                    start_date = None
                    mid_date = None
                    break
    # If current value of CI is greater than 38.2 and we have a mid_date (i.e. we have found a mid_date but not an
    # end_date yet), set mid_date to None to indicate that we need to search for a new mid_date.
    elif df['CI'].iloc[i] > 38.2 and mid_date is not None:
        mid_date = None


# Save the results to a CSV file
df.to_csv('USDJPY_choppiness_index_new.csv')
