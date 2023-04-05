import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# Define the rolling window, standard deviation thresholds, and OHLC_data frequency
roll = 252 * 1
st_dev1 = 2
st_dev2 = 3
period = '1D'

# Load the OHLC_data from a CSV file and set the date column as the index
df = pd.read_csv("Input data/OHLC_data/XAUUSD/XAUUSD1440.csv",
                 names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
df.set_index('date', inplace=True)

# Remove unnecessary columns
df.drop(columns=['time', 'volume'], inplace=True)

# Calculate the price movements and their absolute values
df['move'] = df['close'] - df['close'].shift()
df['abs_move'] = abs(df['close'] - df['close'].shift())

# Calculate the rolling standard deviation of the price movements and the rolling mean of their absolute values
df['st_dev'] = df['move'].rolling(roll).std()
df['avg_move'] = df['abs_move'].rolling(roll).mean()

# Calculate the ratio of price movements to their standard deviation
df['st_dev_move'] = abs(df['move'] / df['st_dev'])

# Calculate the percentage price movements and their absolute values
df['pct_move'] = (df['close'] - df['close'].shift()) / df['close'].shift()
df['abs_pct_move'] = abs((df['close'] - df['close'].shift()) / df['close'].shift())

# Calculate the rolling standard deviation of the percentage price movements
df['st_dev_pct'] = df['pct_move'].rolling(roll).std()

# Calculate the ratio of percentage price movements to their standard deviation
df['st_dev_pct_move'] = abs(df['pct_move'] / df['st_dev_pct'])

# Calculate the differences between daily highs and lows
df['diff'] = df['high'] - df['low']

# Calculate the rolling mean of the daily differences
df['avg_diff'] = df['diff'].rolling(roll).mean()

# Calculate the ratio of daily differences to their rolling mean
df['avg_diff_multiple'] = df['diff'] / df['avg_diff']

# Create binary columns based on whether the price movements, percentage price movements, and daily differences are
# greater than or equal to the corresponding standard deviation threshold
df['2_st_dev_move'] = np.where(df['st_dev_move'] >= st_dev1, 1, 0)
df['3_st_dev_move'] = np.where(df['st_dev_move'] >= st_dev2, 1, 0)
df['2_st_dev_pct_move'] = np.where(df['st_dev_pct_move'] >= st_dev1, 1, 0)
df['3_st_dev_pct_move'] = np.where(df['st_dev_pct_move'] >= st_dev2, 1, 0)
df['2_mean_diff'] = np.where(df['avg_diff_multiple'] >= st_dev1, 1, 0)
df['3_mean_diff'] = np.where(df['avg_diff_multiple'] >= st_dev2, 1, 0)

# Create columns containing the prices where the corresponding binary columns are equal to 1
df['2_st_dev_move_price'] = np.where(df['2_st_dev_move'] == 1, df['close'], np.NAN)
df['3_st_dev_move_price'] = np.where(df['3_st_dev_move'] == 1, df['close'], np.NAN)
df['2_st_dev_pct_move_price'] = np.where(df['2_st_dev_pct_move'] == 1, df['close'], np.NAN)
df['3_st_dev_pct_move_price'] = np.where(df['3_st_dev_pct_move'] == 1, df['close'], np.NAN)
df['2_mean_diff_move_price'] = np.where(df['2_mean_diff'] == 1, df['close'], np.NAN)
df['3_mean_diff_move_price'] = np.where(df['3_mean_diff'] == 1, df['close'], np.NAN)

# Convert the index to a datetime format
df.index = pd.to_datetime(df.index)

# Query the dataframe to select the rows where the binary columns are equal to 1
df2_move = df.query("`2_st_dev_move` == 1")
df3_move = df.query("`3_st_dev_move` == 1")
df2_pct_move = df.query("`2_st_dev_pct_move` == 1")
df3_pct_move = df.query("`3_st_dev_pct_move` == 1")
df2_diff = df.query("`2_mean_diff` == 1")
df3_diff = df.query("`3_mean_diff` == 1")

# write them to CSV files
df.to_csv('results/st_dev_results/st_dev_moves_processed_data/XAUUSD.csv')
df2_move.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_2st_dev_move{roll}.csv')
df3_move.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_3st_dev_move{roll}.csv')
df2_pct_move.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_2st_dev_pct_move{roll}.csv')
df3_pct_move.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_3st_dev_pct_move{roll}.csv')
df2_diff.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_2mean_diff{roll}.csv')
df3_diff.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_3mean_diff{roll}.csv')

# Plot the close price OHLC_data along with the threshold crossings using scatter plots and save them as PNG files
df[['close']].plot()
plt.xlabel('date', fontsize=18)
plt.ylabel('close', fontsize=18)
plt.scatter(df.index, df['2_st_dev_move_price'], color='purple', label='2_st', marker='^', alpha=1)
plt.savefig(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_2st_dev_move{roll}.png')
# plt.show()
df[['close']].plot()
plt.xlabel('date', fontsize=18)
plt.ylabel('close', fontsize=18)
plt.scatter(df.index, df['3_st_dev_move_price'], color='red', label='3_st', marker='v', alpha=1)
plt.savefig(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_3st_dev_move{roll}.png')
# plt.show()

df[['close']].plot()
plt.xlabel('date', fontsize=18)
plt.ylabel('close', fontsize=18)
plt.scatter(df.index, df['2_st_dev_pct_move_price'], color='purple', label='2_st', marker='^', alpha=1)
plt.savefig(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_2st_dev_pct_move{roll}.png')
# plt.show()
df[['close']].plot()
plt.xlabel('date', fontsize=18)
plt.ylabel('close', fontsize=18)
plt.scatter(df.index, df['3_st_dev_pct_move_price'], color='red', label='3_st', marker='v', alpha=1)
plt.savefig(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_3st_dev_pct_move{roll}.png')
# plt.show()

df[['close']].plot()
plt.xlabel('date', fontsize=18)
plt.ylabel('close', fontsize=18)
plt.scatter(df.index, df['2_mean_diff_move_price'], color='purple', label='2_st', marker='^', alpha=1)
plt.savefig(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_2mean_diff{roll}.png')
# plt.show()
df[['close']].plot()
plt.xlabel('date', fontsize=18)
plt.ylabel('close', fontsize=18)
plt.scatter(df.index, df['3_mean_diff_move_price'], color='red', label='3_st', marker='v', alpha=1)
plt.savefig(f'results/st_dev_results/st_dev_moves_processed_data/XAUUSD_3mean_diff{roll}.png')
# plt.show()
