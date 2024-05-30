#%%
import pandas as pd
import statsapi
import time
import datetime as dt
from datetime import timedelta

try:
    staging_df = pd.read_csv('raw_game_data.csv', index_col=False)
    start_date = pd.to_datetime(staging_df['game_date'].max()).date()
except Exception as e:
    print(f'Failed to Read CSV - {e}')
    staging_df = pd.DataFrame()
    start_date = dt.date(2020,1,1)

end_date = dt.date.today()

if start_date != end_date and start_date.year != end_date.year:
    print('Collecting Multiple Seasons of Data.')
    t0 = time.time()
    for year in range(start_date.year, (end_date.year + 1)):
        loop_start_date = dt.date(year, 1, 1)
        loop_end_date = dt.date(year, 12, 31) if dt.date(year, 12, 31) < end_date else end_date
        games = pd.json_normalize(statsapi.schedule(start_date=loop_start_date, end_date=loop_end_date))
        games = games[games['game_type'] == 'R'].copy()
        staging_df = pd.concat([staging_df, games])
    t1 = time.time()
    print(f"All Data Collected in: {round((t1-t0)/60/60, 2)} Hours")
elif start_date != end_date and start_date.year == end_date.year:
    print(f'Updating Data from {start_date} to Today')
    t0 = time.time()
    games = pd.json_normalize(statsapi.schedule(start_date=start_date, end_date=end_date))
    games = games[games['game_type'] == 'R'].copy()
    t1 = time.time()
    print(f"All Data Collected in: {round((t1-t0)/60/60, 2)} Hours")
    staging_df = staging_df[staging_df['game_date'] != start_date].copy()
    staging_df = pd.concat([staging_df, games])
else:
    print("Recollecting Today's Data")
    t0 = time.time()
    games = pd.json_normalize(statsapi.schedule(date=end_date))
    games = games[games['game_type'] == 'R'].copy()
    t1 = time.time()
    print(f"All Data Collected in: {round((t1-t0)/60/60, 2)} Hours")
    staging_df = staging_df[staging_df['game_date'] != end_date].copy()
    staging_df = pd.concat([staging_df, games])
    pass

staging_df.replace('Cleveland Indians', 'Cleveland Guardians', inplace=True)
staging_df = staging_df[['game_id', 'game_date', 'away_name', 'home_name',
                            'home_probable_pitcher', 'away_probable_pitcher',
                            'away_score', 'home_score', 'venue_name']].copy()
staging_df['winning_team'] = [row['home_name'] if row['home_score'] > row['away_score'] else row['away_name'] for _, row in staging_df.iterrows()]
staging_df['losing_team'] = [row['home_name'] if row['home_score'] > row['away_score'] else row['away_name'] for _, row in staging_df.iterrows()]
staging_df.sort_values(['game_date', 'game_id'], inplace=True)
staging_df.drop_duplicates('game_id', keep='last', inplace=True)
staging_df.to_csv('raw_game_data.csv', index=False)

#%%