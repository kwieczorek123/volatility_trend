import pandas as pd

df_gbp_2 = pd.read_csv('GBPUSD_2st_dev_move.csv')
df_eur_2 = pd.read_csv('EURUSD_2st_dev_pct_move.csv')
df_xau_2 = pd.read_csv('XAUUSD_2mean_diff.csv')

df_gbp_3 = pd.read_csv('GBPUSD_3st_dev_move.csv')
df_eur_3 = pd.read_csv('EURUSD_3st_dev_pct_move.csv')
df_xau_3 = pd.read_csv('XAUUSD_3mean_diff.csv')

roll = 252 * 1


df_eur_gbp_2 = pd.merge(left=df_eur_2, left_on='date', right=df_gbp_2, right_on='date')
df_eur_gbp_2.to_csv(f'EURvsGBP_2st_dev_roll={roll}.csv')
with open(f'EURvsGBP_2st_dev_roll={roll}.txt', 'w') as f:
    f.write(df_eur_gbp_2.to_string())
df_eur_gbp_3 = pd.merge(left=df_eur_3, left_on='date', right=df_gbp_3, right_on='date')
df_eur_gbp_3.to_csv(f'EURvsGBP_3st_dev_roll={roll}.csv')
with open(f'EURvsGBP_3st_dev_roll={roll}.txt', 'w') as f:
    f.write(df_eur_gbp_3.to_string())


df_eur_xau_2 = pd.merge(left=df_eur_2, left_on='date', right=df_xau_2, right_on='date')
df_eur_xau_2.to_csv(f'EURvsXAU_2st_dev_roll={roll}.csv')
with open(f'EURvsXAU_2st_dev_roll={roll}.txt', 'w') as f:
    f.write(df_eur_xau_2.to_string())
df_eur_xau_3 = pd.merge(left=df_eur_3, left_on='date', right=df_xau_3, right_on='date')
df_eur_xau_3.to_csv(f'EURvsXAU_3st_dev_roll={roll}.csv')
with open(f'EURvsXAU_3st_dev_roll={roll}.txt', 'w') as f:
    f.write(df_eur_xau_3.to_string())

df_xau_gbp_2 = pd.merge(left=df_xau_2, left_on='date', right=df_gbp_2, right_on='date')
df_xau_gbp_2.to_csv(f'XAUvsGBP_2st_dev_roll={roll}.csv')
with open(f'XAUvsGBP_2st_dev_roll={roll}.txt', 'w') as f:
    f.write(df_xau_gbp_2.to_string())
df_xau_gbp_3 = pd.merge(left=df_xau_3, left_on='date', right=df_gbp_3, right_on='date')
df_xau_gbp_3.to_csv(f'XAUvsGBP_3st_dev_roll={roll}.csv')
with open(f'XAUvsGBP_3st_dev_roll={roll}.txt', 'w') as f:
    f.write(df_xau_gbp_3.to_string())
