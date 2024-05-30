#%%
import pandas as pd
import datetime as dt
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn.model_selection import train_test_split

pr_by_date = pd.read_csv('power_ratings.csv', index_col=0)
betting_data_df = pd.read_csv('hist_betting_data.csv')
games = pd.read_csv('raw_game_data.csv')

pr_types = list(set([x.split('_')[-1] for x in pr_by_date.columns]))
games_pr = games[games['game_date'].isin(list(pr_by_date.index))].copy().reset_index(drop=True)
for pr_type in pr_types:
    games_pr[[f"away_name_{pr_type}", f"home_name_{pr_type}"]] = None

games_pr.fillna('N/A', inplace=True)
for i in range(len(games_pr.index)):
    for pr_type in pr_types:
        try:
            games_pr.at[i, f"away_name_{pr_type}"] = pr_by_date[f"{games_pr.iloc[i]['away_name']}_{pr_type}"][games_pr.iloc[i]['game_date']]
            games_pr.at[i, f"home_name_{pr_type}"] = pr_by_date[f"{games_pr.iloc[i]['home_name']}_{pr_type}"][games_pr.iloc[i]['game_date']]
            games_pr.at[i, f"run_diff_{pr_type}"] = pr_by_date[f"{games_pr.iloc[i]['home_name']}_{pr_type}"][games_pr.iloc[i]['game_date']] + pr_by_date[f"{games_pr.iloc[i]['away_name']}_{pr_type}"][games_pr.iloc[i]['game_date']] + pr_by_date[f"{'HFA'}_{pr_type}"][games_pr.iloc[i]['game_date']]

        except Exception as e:
            print(e)
            pass

games_pr['winning_team_target'] = [1 if row['winning_team'] == row['home_name'] else 0 for _, row in games_pr.iterrows()]
games_pr_today = games_pr[games_pr['game_date'] == dt.date.today().strftime('%Y-%m-%d')].copy()

full = pd.merge(games_pr, betting_data_df, how='left', on=['game_date', 'away_name', 'home_name', 'away_score', 'home_score'])
full.drop_duplicates('game_id', keep=False, inplace=True)
full.dropna(inplace=True)

# Positive x means that home team won by x runs
full['h_run_diff'] = full['home_score'] - full['away_score']

x_drop_cols = ['game_id', 'game_date', 'away_name', 'home_name',
               'home_probable_pitcher', 'away_probable_pitcher', 'away_score',
               'home_score', 'venue_name', 'winning_team', 'winning_team_target', 'losing_team',
               'h_run_diff']
x_train, x_test, y_train, y_test = train_test_split(full, full[['winning_team_target', 'h_run_diff']], test_size=0.5, random_state=5)

orig_x_test = x_test.copy()
half_idx = int((orig_x_test.shape[0]) / 2)
x_practice = orig_x_test[half_idx:].copy()
y_practice = y_test[half_idx:].copy()
x_test = orig_x_test[:half_idx].copy()
y_test = y_test[:half_idx].copy()

res = x_test.copy()

rfr_win = RandomForestRegressor(random_state=5)
rfr_win.fit(x_train.drop(x_drop_cols, axis=1), y_train['winning_team_target'])
pred = rfr_win.predict(x_test.drop(x_drop_cols, axis=1))
res['RFR_Win_Pred'] = pred

ada_win = AdaBoostRegressor(random_state=5)
ada_win.fit(x_train.drop(x_drop_cols, axis=1), y_train['winning_team_target'])
pred = ada_win.predict(x_test.drop(x_drop_cols, axis=1))
res['ADA_Win_Pred'] = pred

rfr_rdiff = RandomForestRegressor(random_state=5)
rfr_rdiff.fit(x_train.drop(x_drop_cols, axis=1), y_train['h_run_diff'])
pred = rfr_rdiff.predict(x_test.drop(x_drop_cols, axis=1))
res['RFR_RDiff_Pred'] = pred

ada_rdiff = AdaBoostRegressor(random_state=5)
ada_rdiff.fit(x_train.drop(x_drop_cols, axis=1), y_train['h_run_diff'])
pred = ada_rdiff.predict(x_test.drop(x_drop_cols, axis=1))
res['ADA_RDiff_Pred'] = pred

res['RFR_Win_classed'] = [1 if row['RFR_Win_Pred'] >= 0.5 else 0 for _, row in res.iterrows()]
res['ADA_Win_classed'] = [1 if row['ADA_Win_Pred'] >= 0.5 else 0 for _, row in res.iterrows()]
res['RFR_Win_grouped'] = round(res['RFR_Win_Pred'], 1)
res['ADA_Win_grouped'] = round(res['ADA_Win_Pred'], 1)
res['ADA_v_RFR_Win'] = [1 if row['ADA_Win_Pred'] - row['RFR_Win_Pred'] >= 0 else 0 for _, row in res.iterrows()]
res['RFR_v_Fair_Win'] = [1 if row['RFR_Win_Pred'] - row['H_fair_odds'] >= 0 else 0  for _, row in res.iterrows()]
res['ADA_v_Fair_Win'] = [1 if row['ADA_Win_Pred'] - row['H_fair_odds'] >= 0 else 0  for _, row in res.iterrows()]

res['Combo_Win_Pred'] = [f"{row['RFR_Win_grouped']}{row['ADA_Win_grouped']}{row['ADA_v_RFR_Win']}{row['RFR_v_Fair_Win']}{row['ADA_v_Fair_Win']}" for _, row in res.iterrows()]
crosstab_df = pd.crosstab(res['Combo_Win_Pred'], res['winning_team_target'])
crosstab_df['maj'] = [1 if row[1] > row[0] else 0 for _, row in crosstab_df.iterrows()]

practice_rfr_win_pred = rfr_win.predict(x_practice.drop(x_drop_cols, axis=1))
practice_ada_win_pred = ada_win.predict(x_practice.drop(x_drop_cols, axis=1))
practice_rfr_rdiff_pred = rfr_rdiff.predict(x_practice.drop(x_drop_cols, axis=1))
practice_ada_rdiff_pred = ada_rdiff.predict(x_practice.drop(x_drop_cols, axis=1))

x_practice['RFR_Win_Pred'] = practice_rfr_win_pred
x_practice['ADA_Win_Pred'] = practice_ada_win_pred
x_practice['RFR_RDiff_Pred'] = practice_rfr_rdiff_pred
x_practice['ADA_RDiff_Pred'] = practice_ada_rdiff_pred
x_practice['RFR_Win_classed'] = [1 if row['RFR_Win_Pred'] >= 0.5 else 0 for _, row in x_practice.iterrows()]
x_practice['ADA_Win_classed'] = [1 if row['ADA_Win_Pred'] >= 0.5 else 0 for _, row in x_practice.iterrows()]
x_practice['RFR_Win_grouped'] = round(x_practice['RFR_Win_Pred'], 1)
x_practice['ADA_Win_grouped'] = round(x_practice['ADA_Win_Pred'], 1)
x_practice['ADA_v_RFR_Win'] = [1 if row['ADA_Win_Pred'] - row['RFR_Win_Pred'] >= 0 else 0 for _, row in x_practice.iterrows()]
x_practice['RFR_v_Fair_Win'] = [1 if row['RFR_Win_Pred'] - row['H_fair_odds'] >= 0 else 0  for _, row in x_practice.iterrows()]
x_practice['ADA_v_Fair_Win'] = [1 if row['ADA_Win_Pred'] - row['H_fair_odds'] >= 0 else 0  for _, row in x_practice.iterrows()]

x_practice['Combo_Win_Pred'] = [f"{row['RFR_Win_grouped']}{row['ADA_Win_grouped']}{row['ADA_v_RFR_Win']}{row['RFR_v_Fair_Win']}{row['ADA_v_Fair_Win']}" for _, row in x_practice.iterrows()]
x_practice = pd.merge(x_practice, crosstab_df, on='Combo_Win_Pred')
x_practice['Combo_H_W%'] = x_practice[1] / (x_practice[0] + x_practice[1])

x_practice.drop_duplicates('game_id', inplace=True)

x_practice[['away_name', 'home_name', 'Combo_Win_Pred', 0, 1, 'maj', 'Combo_H_W%']]

games_pr_today.drop(['home_probable_pitcher', 'away_probable_pitcher',
                     'venue_name', 'winning_team', 'losing_team'],
                     axis=1, inplace=True)
games_pr_today.drop_duplicates('game_id', inplace=True)
todays_odds = pd.read_csv('today_betting_data.csv', index_col=False)
gm_odds = pd.merge(games_pr_today, todays_odds, how='left', on=['game_date', 'away_name', 'home_name'])
today_rfr_win_pred = rfr_win.predict(gm_odds.fillna(0).drop(['game_id', 'game_date', 'away_name', 'home_name','away_score', 'home_score', 'winning_team_target'], axis=1))
today_ada_win_pred = ada_win.predict(gm_odds.fillna(0).drop(['game_id', 'game_date', 'away_name', 'home_name','away_score', 'home_score', 'winning_team_target'], axis=1))
today_rfr_rdiff_pred = rfr_rdiff.predict(gm_odds.fillna(0).drop(['game_id', 'game_date', 'away_name', 'home_name','away_score', 'home_score', 'winning_team_target'], axis=1))
today_ada_rdiff_pred = ada_rdiff.predict(gm_odds.fillna(0).drop(['game_id', 'game_date', 'away_name', 'home_name','away_score', 'home_score', 'winning_team_target'], axis=1))

gm_odds['RFR_Win_Pred'] = today_rfr_win_pred
gm_odds['ADA_Win_Pred'] = today_ada_win_pred
gm_odds['RFR_RDiff_Pred'] = today_rfr_rdiff_pred
gm_odds['ADA_RDiff_Pred'] = today_ada_rdiff_pred
gm_odds['RFR_Win_classed'] = [1 if row['RFR_Win_Pred'] >= 0.5 else 0 for _, row in gm_odds.iterrows()]
gm_odds['ADA_Win_classed'] = [1 if row['ADA_Win_Pred'] >= 0.5 else 0 for _, row in gm_odds.iterrows()]
gm_odds['RFR_Win_grouped'] = round(gm_odds['RFR_Win_Pred'], 1)
gm_odds['ADA_Win_grouped'] = round(gm_odds['ADA_Win_Pred'], 1)
gm_odds['ADA_v_RFR_Win'] = [1 if row['ADA_Win_Pred'] - row['RFR_Win_Pred'] >= 0 else 0 for _, row in gm_odds.iterrows()]
gm_odds['RFR_v_Fair_Win'] = [1 if row['RFR_Win_Pred'] - row['H_fair_odds'] >= 0 else 0  for _, row in gm_odds.iterrows()]
gm_odds['ADA_v_Fair_Win'] = [1 if row['ADA_Win_Pred'] - row['H_fair_odds'] >= 0 else 0  for _, row in gm_odds.iterrows()]

gm_odds['Combo_Win_Pred'] = [f"{row['RFR_Win_grouped']}{row['ADA_Win_grouped']}{row['ADA_v_RFR_Win']}{row['RFR_v_Fair_Win']}{row['ADA_v_Fair_Win']}" for _, row in gm_odds.iterrows()]
gm_odds = pd.merge(gm_odds, crosstab_df, on='Combo_Win_Pred')
gm_odds['Combo_H_W%'] = gm_odds[1] / (gm_odds[0] + gm_odds[1])

gm_odds.drop_duplicates('game_id', inplace=True)

gm_odds[['away_name', 'home_name', 'RFR_Win_classed', 'ADA_Win_classed', 'RFR_RDiff_Pred',
         'ADA_RDiff_Pred', 'Combo_Win_Pred', 0, 1, 'maj', 'Combo_H_W%']]
#%%