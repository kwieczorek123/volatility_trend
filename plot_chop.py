import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Load data from Excel file
df = pd.read_excel('USDJPY_choppiness_index_new_amended.xlsx')

# Step 2: Create a new column for color based on the 'trend' column
df['color'] = df['trend'].map({0: 'black', 1: 'green'})

# Step 3: Group the data by the date and the color column, and calculate the mean close price for each group
grouped = df.groupby(['date', 'color']).mean()['close'].unstack()

# Step 4: Plot the data as a line chart, with the color determined by the color column
fig, ax = plt.subplots()
grouped.plot(ax=ax, color=['black', 'green'])
ax.set_xlabel('Date')
ax.set_ylabel('Close')
ax.legend(['Not trending', 'Trending'], loc='upper left')

plt.savefig('USDJPY_trend.png')
plt.show()



