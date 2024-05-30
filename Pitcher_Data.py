#%%
import pandas as pd
import statsapi
import time
import datetime as dt
from datetime import timedelta

def To_Thirds(inn):
    inn = float(inn)
    q, r = divmod(inn, 1)
    thirds = round(r * 10, 0) + (q * 3)
    return thirds

def To_Innings(thirds):
    q, r = divmod(thirds, 3)
    if r:
        r = r/10
    return round(q+r, 1)

raw_game_data = pd.read_csv('raw_game_data.csv', index_col=False)

# try:
#     k_data = pd.read_csv('pitcher_data.csv', index_col=False)
#     missing_k_data_games = raw_game_data[~raw_game_data['game_id'].isin(k_data['game_id'])]['game_id'].tolist() + k_data[(k_data['p_away'].isna()) | (k_data['h_away'].isna())]['game_id'].to_list()
# except Exception as e:
#     print(f'Failed to Read CSV - {e}')
k_data = pd.DataFrame()
missing_k_data_games = raw_game_data['game_id'].to_list()

t0 = time.time()
print('Collecting Missing K Data.')
for game_id in missing_k_data_games[:3]:
    try:
        bsd = statsapi.boxscore_data(game_id)

        a_p = pd.DataFrame(pd.json_normalize(bsd['awayPitchers']))[['ip', 'h', 'r', 'bb', 'k', 'hr']]
        h_p = pd.DataFrame(pd.json_normalize(bsd['homePitchers']))[['ip', 'h', 'r', 'bb', 'k', 'hr']]

        a_sp, a_rp = a_p.iloc[1].copy(), a_p[2:].copy()
        h_sp, h_rp = h_p.iloc[1].copy(), h_p[2:].copy()
        
        for df in [a_sp, a_rp, h_sp, h_rp]:
            df[['ip', 'h', 'r', 'bb', 'k', 'hr']] = df[['ip', 'h', 'r', 'bb', 'k', 'hr']].astype(float)
            if type(df) != pd.Series:
                df['ip'] = df['ip'].apply(To_Thirds)

        a_rp = a_rp[['ip', 'h', 'r', 'bb', 'k', 'hr']].sum()
        h_rp = h_rp[['ip', 'h', 'r', 'bb', 'k', 'hr']].sum()

        a_rp['ip'] = To_Innings(a_rp['ip'])
        h_rp['ip'] = To_Innings(h_rp['ip'])

        a_sp['p'] = 'a_sp'
        a_rp['p'] = 'a_rp'
        h_sp['p'] = 'h_sp'
        h_rp['p'] = 'h_rp'

        for df in [a_sp, a_rp, h_sp, h_rp]:
            rename_dict = {col: f"{df['p']}_{col}" for col in df.columns[:-1]}
            df.rename(columns=rename_dict)

        temp = pd.concat([a_sp, a_rp, h_sp, h_rp])
        k_data = pd.concat([k_data, temp], axis=0)
    except Exception as e:
        print(f"Data not collected - {e}")
        if game_id not in k_data['game_id'].to_list():
            k_data = pd.concat([k_data, pd.DataFrame(data={'game_id': [game_id]})], axis=0)
        else:
            pass
#%%
t1 = time.time()
print(f"All Data Collected in: {round((t1-t0)/60/60, 2)} Hours")

k_data.to_csv('k_data.csv', index=False)

#%%