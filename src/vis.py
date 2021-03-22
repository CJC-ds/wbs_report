import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import constant

today = datetime.now()
today_str = datetime.strftime(today, '%y%m%d')
today_str_hr = datetime.strftime(today, '%Y-%m-%d')

delta = timedelta(days=constant.TIMEFRAME)

today_delta = today - delta7
today_delta_str = datetime.strftime(today_delta, '%y%m%d')
today_delta_str_hr = datetime.strftime(today_delta, '%Y-%m-%d')

# path = os.path.join('data')
df = pd.read_csv(f'./data/raw/{today_delta_str}_{today_str}_raw.csv')

df['CREATED_UTC'] = pd.to_datetime(df.CREATED_UTC, infer_datetime_format=True)
top20 = df.TAG_MERGE.value_counts()[:20].index.values.tolist()
df20 = df[df['TAG_MERGE'].isin(top20)].copy().reset_index()


# df20_melt = df20.resample('D', on='CREATED_UTC')\
# .TAG_MERGE.value_counts()\
# .unstack()\
# .fillna(0)\
# .stack()\
# .reset_index(name='MENTIONS')\
# .rename(columns={'level_1': 'TICKER', 'TAG_MERGE':'TICKER_SYMBOL'})

# fig, ax = plt.subplots(figsize=(12,12))
# sns.barplot(x='CREATED_UTC', y='MENTIONS', hue='TICKER_SYMBOL', data=df20_melt, ax=ax)
# x_dates = df20_melt.CREATED_UTC.dt.strftime('%Y-%m-%d').sort_values().unique()
# ax.set_xticklabels(labels=x_dates, rotation=60)
# plt.show()

fig, ax = plt.subplots()
# xlocs, xlabs = plt.xticks()
# for i, v in enumerate(df20.TAG_MERGE.value_counts().values.tolist()):
#     plt.text(xlocs[i] - 0.25, v + 0.01, str(v))
y = df20.TAG_MERGE.value_counts().sort_values(ascending=True)
y.plot(kind='barh', figsize=(12, 12), ax=ax,
       title=f'Top20 stocks mentioned in r/WallStreetBets posts.\n(In the last 7 days\n{today_delta_str_hr} to {today_str_hr})')
y_list = y.values.tolist()
_, xmax = plt.xlim()
plt.xlim(0, xmax+300)
for i, v in enumerate(y_list):
    ax.text(v + 100, i, str(v), color='black', fontweight='bold', ha='left', fontsize=14, va='center')

plt.savefig(f'./plots/{today_str_hr}.png')
plt.show()
