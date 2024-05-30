#%%
import pandas as pd
from threading import Thread
from datetime import datetime
import time
from sklearn.linear_model import Ridge

class PowerRatingCreation(Thread):
    def __init__(self, full_df, pr_len, dates):
        super().__init__()
        self.pr_len = pr_len
        self.full_df = full_df
        self.dates = dates
        self.start_date_index = 0
        self.full_power_ratings = pd.DataFrame()

    def run(self):
        if type(self.pr_len) != str:
            for date_index in range(self.pr_len, len(dates)):
                start_date = dates[self.start_date_index]
                end_date = dates[date_index - 1]
                pr_date = dates[date_index]
                pr_day_df = self.full_df[(self.full_df['game_date'] >= start_date) & (self.full_df['game_date'] <= end_date)]

                RidReg = Ridge(fit_intercept=False, random_state=5)
                RidReg.fit(pr_day_df.drop(['game_date', 'game_id', 'Target'], axis=1), pr_day_df['Target'])

                if end_date == datetime.today().strftime('%Y-%m-%d'):
                    self.todays_pr = dict(list(zip(cols[:-3], RidReg.coef_)))
                else:
                    pr = {f'{team}_{self.pr_len}': rating for team, rating in zip(cols[:-3], RidReg.coef_)}
                    pr['game_date'] = pr_date
                    temp_pr = pd.DataFrame([pr])
                    self.full_power_ratings = pd.concat([self.full_power_ratings, temp_pr])

                self.start_date_index += 1
        else:
            for date_index in range(len(dates)):
                start_date = dates[self.start_date_index]
                end_date = dates[date_index - 1]
                pr_date = dates[date_index]
                pr_day_df = self.full_df[(self.full_df['game_date'] < end_date) & (pd.to_datetime(self.full_df['game_date']).dt.year == pd.to_datetime(end_date).year)]

                if pr_day_df.index.size > 0:
                    RidReg = Ridge(fit_intercept=False, random_state=5)
                    RidReg.fit(pr_day_df.drop(['game_date', 'game_id', 'Target'], axis=1), pr_day_df['Target'])

                    if end_date == datetime.today().strftime('%Y-%m-%d'):
                        self.todays_pr = dict(list(zip(cols[:-3], RidReg.coef_)))
                    else:
                        pr = {f'{team}_{self.pr_len}': rating for team, rating in zip(cols[:-3], RidReg.coef_)}
                        pr['game_date'] = pr_date
                        temp_pr = pd.DataFrame([pr])
                        self.full_power_ratings = pd.concat([self.full_power_ratings, temp_pr])

                    self.start_date_index += 1
                else:
                    pass


staging_df = pd.read_csv('raw_game_data.csv', index_col=False)

staging_df.fillna('N/A', inplace=True)

staging_df[['home_score', 'away_score']] = staging_df[['home_score', 'away_score']].apply(pd.to_numeric)
staging_df['total_runs'] = staging_df['away_score'] + staging_df['home_score']

games = []
for _, row in staging_df.iterrows():
    temp = {'game_date': row['game_date'],
            'game_id': row['game_id'],
            row['home_name']: 1,
            row['away_name']: 1,
            row['home_probable_pitcher']: 1,
            row['away_probable_pitcher']: 1,
            row['venue_name']: 1,
            'Target': row['total_runs']}
    games.append(temp)

full = pd.DataFrame(games)
full.fillna(0, inplace=True)

teams = pd.concat([staging_df['home_name'], staging_df['away_name']]).unique()
pitchers = pd.concat([staging_df['home_probable_pitcher'], staging_df['away_probable_pitcher']]).unique()
venues = staging_df['venue_name'].unique()
cols = [t for t in teams] + [p for p in pitchers] + [v for v in venues] + ['Target', 'game_date', 'game_id']

full = full[cols].copy()
full.sort_values(['game_date', 'game_id'], inplace=True)

dates = list(staging_df['game_date'].unique()) + [datetime.today().strftime('%Y-%m-%d')]
pr_splits = ['Season', 7, 15, 30, 60]
pr_threads = [PowerRatingCreation(full, pr_len, dates) for pr_len in pr_splits]

t0 = time.time()
[t.start() for t in pr_threads]
[t.join() for t in pr_threads]
t1 = time.time()

pr_matchups = pr_threads[0].full_power_ratings.copy()
for pr_t in pr_threads[1:]:
    pr_matchups = pd.merge(pr_matchups, pr_t.full_power_ratings, how='left', on='game_date')

pr_matchups.dropna(inplace=True)
pr_matchups.sort_index(axis=1, inplace=True)
pr_matchups['game_date'] = pd.to_datetime(pr_matchups['game_date'])
pr_matchups.set_index('game_date', inplace=True)

pr_matchups.to_csv('total_runs_ratings.csv', index=True)
#%%
todays_date = '2024-05-20'
today_games = staging_df[staging_df['game_date'] == todays_date].copy().reset_index()
today_games['OU'] = 0

#%%
game_ou = {'Padres': 7.5}
for key, value in game_ou.items():
    today_games.iloc[today_games[today_games['away_name'].str.contains(key)].index, -1] = value

#%%
cols = ['away_name', 'home_name', 'home_probable_pitcher', 'away_probable_pitcher', 'venue_name']
all_outputs = {}
for split in pr_splits:
    for _, row in today_games.iterrows():
        res = []
        for col in cols:
            res.append(pr_matchups[f'{row[col]}_{split}']['2024-05-14'])
        bin_res = 1 if sum(res) > row['OU'] else 0
        all_outputs[f'{row["away_name"]} - {row["home_name"]} - {row["home_probable_pitcher"]} ({split})'] = [sum(res), bin_res]

all_outputs = dict(sorted(all_outputs.items()))
cnt = 1
group = []
for key, val in all_outputs.items():
    if cnt % 5 == 0:
        group.append(val[1])
        cnt += 1
        if sum(group) in [0, 5]:
            print(f'{key[:-9]} - {group} - {sum(group)}')
        cnt = 1
        group = []
    else:
        group.append(val[1])
        cnt += 1

#%%