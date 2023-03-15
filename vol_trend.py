import os
import pandas as pd
from datetime import datetime

file_names = ['vol_trend_data/EURUSD_chop.csv', 'vol_trend_data/GBPUSD_chop.csv', 'vol_trend_data/USDJPY_chop.csv',
              'vol_trend_data/XAUUSD_chop.csv']

spread_all_symbols_file = 'vol_trend_data/spread_all_symbol.csv'
execution_spread_file = 'vol_trend_data/execution_spread.csv'


def convert_date_format(file_path, current_format, target_format):
    df = pd.read_csv(file_path)
    df['day'] = pd.to_datetime(df['day'], format=current_format).dt.strftime(target_format)
    df.to_csv(file_path, index=False)


# Conversion for EURUSD and XAUUSD pnl files
eurusd_pnl = 'vol_trend_data/EURUSD_pnl.csv'
xauusd_pnl = 'vol_trend_data/XAUUSD_pnl.csv'

convert_date_format(eurusd_pnl, '%d/%m/%Y', '%m/%d/%Y')
convert_date_format(xauusd_pnl, '%d/%m/%Y', '%m/%d/%Y')

pnl_files = ['vol_trend_data/EURUSD_pnl.csv', 'vol_trend_data/GBPUSD_pnl.csv', 'vol_trend_data/USDJPY_pnl.csv',
             'vol_trend_data/XAUUSD_pnl.csv']
pnl_df_dict = {symbol: pd.read_csv(pnl_file, parse_dates=['day']) for pnl_file, symbol in
               zip(pnl_files, ['EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD'])}

start_date = '2021-01-01'
end_date = '2023-02-28'
roll = 252


def add_volatility_columns(df):
    df['Volatility'] = (df['high'] - df['low']) / df['low']
    df['Volatility_median'] = df['Volatility'].rolling(window=roll).median()
    df['Volatility_status'] = df.apply(
        lambda row: 'High_Volatility' if row['Volatility'] > row['Volatility_median'] else 'Low_Volatility', axis=1)
    return df


def filter_date_range(df, start_date, end_date):
    date_mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    return df[date_mask]


def mark_volatility_trend(df):
    df['Volatility_Trend'] = df.apply(
        lambda row: 'High Volatility + Trend' if row['Volatility_status'] == 'High_Volatility' and row[
            'trend'] == 1
        else ('High Volatility + No Trend' if row['Volatility_status'] == 'High_Volatility' and row['trend'] != 1
              else ('Low Volatility + Trend' if row['Volatility_status'] == 'Low_Volatility' and row['trend'] == 1
                    else 'Low Volatility + No Trend')), axis=1)
    return df


def add_spread_columns(df, spread_df, execution_df, symbol):
    spread_factors = {'EURUSD': 100000, 'GBPUSD': 100000, 'USDJPY': 1000, 'XAUUSD': 100}

    df = df.merge(spread_df.loc[spread_df['symbol'] == symbol, ['day', 'spread']], left_on='date', right_on='day',
                  how='left').drop(columns=['day'])
    df = df.rename(columns={'spread': 'typical_spread_in_points'})
    # Multiply the typical_spread column by the appropriate factor
    df['typical_spread_in_points'] = df['typical_spread_in_points'] * spread_factors[symbol]

    df = df.merge(execution_df.loc[execution_df['symbol_name'] == symbol, ['date', 'vol_avg_spread']], on='date',
                  how='left')
    df = df.rename(columns={'vol_avg_spread': 'weighted_avg_execution_spread_$'})

    return df


def add_pnl_per_lot_column(df, pnl_df, symbol):
    df = df.merge(pnl_df.loc[pnl_df['symbol_name'] == symbol, ['day', 'PnL/Lot']], left_on='date', right_on='day',
                  how='left')
    df = df.rename(columns={'PnL/Lot': 'PnL_per_lot'})
    df.drop(columns=['day'], inplace=True)
    return df


def process_csv(file_name, start_date, end_date, spread_df, execution_df, pnl_df_dict):
    symbol = file_name.split('/')[-1].split('_')[0]  # Extract the symbol from the file name
    df = pd.read_csv(file_name, parse_dates=['date'])
    df = add_volatility_columns(df)
    df = filter_date_range(df, start_date, end_date)
    df = mark_volatility_trend(df)
    df = add_spread_columns(df, spread_df, execution_df, symbol)
    df = add_pnl_per_lot_column(df, pnl_df_dict[symbol], symbol)
    output_dir = "processed_vol_trend_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file_path = os.path.join(output_dir, f'processed_{os.path.basename(file_name)}')
    df.to_csv(output_file_path, index=False)
    print(f"Processed {file_name}")


spread_df = pd.read_csv(spread_all_symbols_file, parse_dates=['day'])
execution_df = pd.read_csv(execution_spread_file, parse_dates=['date'])

for file_name in file_names:
    process_csv(file_name, start_date, end_date, spread_df, execution_df, pnl_df_dict)
