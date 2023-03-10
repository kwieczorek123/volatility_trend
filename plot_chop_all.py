import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Define a list of symbols to plot
symbols = ['EURUSD', 'GBPUSD', 'XAUUSD', 'USDJPY']

# Step 2: Create a grid of subplots with the same number of rows as symbols and one column
fig, axs = plt.subplots(nrows=len(symbols), ncols=1, figsize=(80, 50), sharex=True)

# Step 3: Iterate over each symbol and load and plot the data in the appropriate subplot
for i, symbol in enumerate(symbols):
    # Load data from Excel file
    df = pd.read_excel(f'{symbol}_choppiness_index_new_amended.xlsx')

    # Create a new column for color based on the 'trend' column
    df['color'] = df['trend'].map({0: 'black', 1: 'green'})

    # Group the data by the date and the color column, and calculate the mean close price for each group
    grouped = df.groupby(['date', 'color']).mean()['close'].unstack()

    # Plot the data as a line chart, with the color determined by the color column
    grouped.plot(ax=axs[i], color=['black', 'green'], linewidth=5)
    axs[i].set_xlabel('Date', fontsize=22)
    axs[i].set_ylabel('Close', fontsize=22)
    axs[i].set_title(symbol, fontsize=36)

    # Add text indicating trend status only on the first chart
    if i == 0:
        axs[i].text(0.05, 0.95, 'Trending', transform=axs[i].transAxes, va='top', color='green', fontsize=22,
                    fontweight='bold')
        axs[i].text(0.05, 0.85, 'Not trending', transform=axs[i].transAxes, va='top', color='black', fontsize=22,
                    fontweight='bold')

    # Remove legend and set tick parameters
    axs[i].legend().remove()
    axs[i].tick_params(axis='both', which='major', labelsize=24)
    axs[i].tick_params(axis='both', which='minor', labelsize=24)

# Step 4: Save and show the chart
plt.savefig('all_trends.png', dpi=300, bbox_inches='tight')
plt.show()
