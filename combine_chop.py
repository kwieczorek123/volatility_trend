import pandas as pd

# Step 1: Define a list of symbols to combine
symbols = ['XAUUSD', 'EURUSD', 'GBPUSD', 'USDJPY']

# Step 2: Load data from Excel files and append them into one DataFrame
combined_df = pd.DataFrame()
for symbol in symbols:
    df = pd.read_excel(f'{symbol}_choppiness_index_new_amended.xlsx', usecols=['date', 'trend'])
    df['symbol'] = symbol
    combined_df = pd.concat([combined_df, df])

# Step 3: Group the data by the date and sum the trend values for each group
grouped = combined_df.groupby('date')['trend'].sum()

# Step 4: Save the data to a CSV file
grouped.to_csv('trends_overlapping_top4_1.csv')
