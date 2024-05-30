#%%
import pandas as pd
from threading import Thread
from datetime import datetime
import time
from sklearn.linear_model import Ridge

class PowerRatingCreation(Thread):
    def __init__(self, full_df, pr_len, pitchers):
        super().__init__()
        self.pr_len = pr_len
        self.full_df = full_df
        self.pitchers = pitchers
        self.start_date_index = 0
        self.full_power_ratings = pd.DataFrame()

    def run(self):
        if type(self.pr_len) != str:
            for pitcher in self.pitchers:
                # start_date = dates[self.start_date_index]
                # end_date = dates[date_index - 1]
                # pr_date = dates[date_index]
                pr_day_df = self.full_df[self.full_df[f"{pitcher}_{self.pr_len}"] == 1].head(self.pr_len)
                # pr_day_df = self.full_df[(self.full_df['game_date'] >= start_date) & (self.full_df['game_date'] <= end_date)]

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
            # for date_index in range(len(dates)):
            #     start_date = dates[self.start_date_index]
            #     end_date = dates[date_index - 1]
            #     pr_date = dates[date_index]
            #     pr_day_df = self.full_df[(self.full_df['game_date'] < end_date) & (pd.to_datetime(self.full_df['game_date']).dt.year == pd.to_datetime(end_date).year)]

            #     if pr_day_df.index.size > 0:
            #         RidReg = Ridge(fit_intercept=False, random_state=5)
            #         RidReg.fit(pr_day_df.drop(['game_date', 'game_id', 'Target'], axis=1), pr_day_df['Target'])

            #         if end_date == datetime.today().strftime('%Y-%m-%d'):
            #             self.todays_pr = dict(list(zip(cols[:-3], RidReg.coef_)))
            #         else:
            #             pr = {f'{team}_{self.pr_len}': rating for team, rating in zip(cols[:-3], RidReg.coef_)}
            #             pr['game_date'] = pr_date
            #             temp_pr = pd.DataFrame([pr])
            #             self.full_power_ratings = pd.concat([self.full_power_ratings, temp_pr])

            #         self.start_date_index += 1
            #     else:
            #         pass

def split_list(lst, num_sections):
    section_size = len(lst) // num_sections
    remaining_elements = len(lst) % num_sections
    sections = []
    start = 0
    for i in range(num_sections):
        if i < remaining_elements:
            end = start + section_size + 1
        else:
            end = start + section_size
        sections.append(lst[start:end])
        start = end
    return sections

raw_game_data = pd.read_csv('raw_game_data.csv', index_col=False)
k_data = pd.read_csv('k_data.csv', index_col=False)

staging_df = pd.merge(left=raw_game_data, right=k_data, on='game_id')

# todays_pitchers = staging_df[staging_df['game_date'] == '2024-04-30']['home_probable_pitcher'].to_list() + staging_df[staging_df['game_date'] == '2024-04-30']['away_probable_pitcher'].to_list()
# staging_df = staging_df[(staging_df['home_probable_pitcher'].isin(todays_pitchers)) | (staging_df['away_probable_pitcher'].isin(todays_pitchers))].copy()

staging_df[['k_home', 'k_away']] = staging_df[['k_home', 'k_away']].apply(pd.to_numeric)
staging_df.fillna(0, inplace=True)

home_pitcher = []
for _, row in staging_df.iterrows():
    temp = {'game_date': row['game_date'],
            'game_id': row['game_id'],
            row['away_name']: 1,
            row['home_probable_pitcher']: 1,
            'Target': row['k_home']}
    home_pitcher.append(temp)

away_pitcher = []
for _, row in staging_df.iterrows():
    temp = {'game_date': row['game_date'],
            'game_id': row['game_id'],
            row['home_name']: 1,
            row['away_probable_pitcher']: 1,
            'Target': row['k_away']}
    away_pitcher.append(temp)

full = pd.concat([pd.DataFrame(home_pitcher), pd.DataFrame(away_pitcher)], axis=0)
full.fillna(0, inplace=True)

pitchers = pd.concat([staging_df['home_probable_pitcher'], staging_df['away_probable_pitcher']]).unique()
teams = pd.concat([staging_df['home_name'], staging_df['away_name']]).unique()
cols = [p for p in pitchers] + [t for t in teams] + ['Target', 'game_date', 'game_id']

full = full[cols].copy()
full.sort_values(['game_date', 'game_id'], inplace=True)
full.columns = full.columns.astype(str)

# dates = list(staging_df['game_date'].unique()) + [datetime.today().strftime('%Y-%m-%d')]
pr_splits = ['Season', 5, 15, 30]
pr_threads = [PowerRatingCreation(full, pr_len, pitchers) for pr_len in pr_splits]

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

pr_matchups.to_csv('k_power_ratings.csv', index=True)
#%%