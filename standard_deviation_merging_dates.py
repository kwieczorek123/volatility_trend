import pandas as pd

df_gbp_2 = pd.read_csv('results/st_dev_results/st_dev_moves_processed_data/GBPUSD_2st_dev_move.csv')
df_eur_2 = pd.read_csv('results/st_dev_results/st_dev_moves_processed_data/EURUSD_2st_dev_pct_move.csv')
df_xau_2 = pd.read_csv('results/st_dev_results/st_dev_moves_processed_data/XAUUSD_2mean_diff.csv')

df_gbp_3 = pd.read_csv('results/st_dev_results/st_dev_moves_processed_data/GBPUSD_3st_dev_move.csv')
df_eur_3 = pd.read_csv('results/st_dev_results/st_dev_moves_processed_data/EURUSD_3st_dev_pct_move.csv')
df_xau_3 = pd.read_csv('results/st_dev_results/st_dev_moves_processed_data/XAUUSD_3mean_diff.csv')

roll = 252 * 1


df_eur_gbp_2 = pd.merge(left=df_eur_2, left_on='date', right=df_gbp_2, right_on='date')
df_eur_gbp_2.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/EURvsGBP_2st_dev_roll={roll}.csv')
df_eur_gbp_3 = pd.merge(left=df_eur_3, left_on='date', right=df_gbp_3, right_on='date')
df_eur_gbp_3.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/EURvsGBP_3st_dev_roll={roll}.csv')


df_eur_xau_2 = pd.merge(left=df_eur_2, left_on='date', right=df_xau_2, right_on='date')
df_eur_xau_2.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/EURvsXAU_2st_dev_roll={roll}.csv')
df_eur_xau_3 = pd.merge(left=df_eur_3, left_on='date', right=df_xau_3, right_on='date')
df_eur_xau_3.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/EURvsXAU_3st_dev_roll={roll}.csv')

df_xau_gbp_2 = pd.merge(left=df_xau_2, left_on='date', right=df_gbp_2, right_on='date')
df_xau_gbp_2.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUvsGBP_2st_dev_roll={roll}.csv')
df_xau_gbp_3 = pd.merge(left=df_xau_3, left_on='date', right=df_gbp_3, right_on='date')
df_xau_gbp_3.to_csv(f'results/st_dev_results/st_dev_moves_processed_data/XAUvsGBP_3st_dev_roll={roll}.csv')
