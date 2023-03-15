import os
import pandas as pd


start_date = '2021-01-01'
end_date = '2023-03-01'
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


def process_csv(file_name, start_date, end_date):
    df = pd.read_csv(file_name, parse_dates=['date'])
    df = add_volatility_columns(df)
    df = filter_date_range(df, start_date, end_date)
    df = mark_volatility_trend(df)
    output_dir = "processed_vol_trend_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file_path = os.path.join(output_dir, f'processed_{os.path.basename(file_name)}')
    df.to_csv(output_file_path, index=False)
    print(f"Processed {file_name}")


file_names = ['vol_trend_data/EURUSD_chop.csv', 'vol_trend_data/GBPUSD_chop.csv', 'vol_trend_data/USDJPY_chop.csv',
              'vol_trend_data/XAUUSD_chop.csv']



for file_name in file_names:
    process_csv(file_name, start_date, end_date)
