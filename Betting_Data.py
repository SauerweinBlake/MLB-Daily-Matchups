#%%
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options

def Pull_Hist_Odds_Data(html):
    soup = BeautifulSoup(html, 'html.parser')

    table_matches = soup.find('table', attrs={'class':'table-main js-tablebanner-t js-tablebanner-ntb'})

    data = []
    rows = table_matches.find_all('tr')
    for row in rows:
        utils = []
        cols = row.find_all('td')
        for element in cols:
            try:
                if 'data-odd' in element.attrs:
                    utils.append(element['data-odd'])
                else:
                    utils.append(element.span.span.span['data-odd'])
            except:
                utils.append(element.text)
        if utils:
            data.append(utils)
    return data


def Pull_Today_Odds_Data(html):
    soup = BeautifulSoup(html, 'html.parser')
    table_matches = soup.find('table', attrs={'class':'table-main table-main--leaguefixtures h-mb15 js-tablebanner-t js-tablebanner-ntb'})

    data = []
    valnames = ['table-main__datetime', 'h-text-left']
    rows = table_matches.find_all('tr')
    for row in rows:
        utils = []
        cols = row.find_all('td')
        for element in cols:
            try:
                if element['class'][0] in valnames:
                    utils.append(element.text)
                elif element['class'][0] == 'table-main__odds':
                    utils.append(element.button.text)
            except:
                pass
        if utils:
            data.append(utils)
    return data

def Transform_DF(df):
    df['Date'] = [f'{date.replace(".", "/")}2024' if date[-1] == '.' else (datetime.today() - timedelta(days=1)).strftime('%d/%m/%Y') if (date == 'Yesterday' or date == 'Today') else date.replace('.', '/') for date in df['Date']]
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')

    df[['Home','Away']] = df['Match'].str.split(' - ', expand=True)
    df[['home_score', 'away_score']] = df['Result'].str.split(':', expand=True)
    df = df[(df['1'] != '\xa0') & (df['2'] != '\xa0') & (df['home_score'] != 'CAN.')][['Date', 'Home', 'Away', 'home_score', 'away_score', '1', '2']].copy()

    df['1'] = pd.to_numeric(df['1'])
    df['2'] = pd.to_numeric(df['2'])
    df['H_odds'] = (1 / df['1'])
    df['A_odds'] = (1 / df['2'])
    df['vig'] = (df['H_odds'] + df['A_odds']) - 1
    df['H_fair_odds'] = df['H_odds'] / (1 + df['vig'])
    df['A_fair_odds'] = df['A_odds'] / (1 + df['vig'])
    df.rename(columns={'1': 'H_Payout', '2': 'A_Payout', 'Date': 'game_date',
                        'Away': 'away_name', 'Home': 'home_name'}, inplace=True)
    
    return df

betting_data_df = pd.DataFrame()

URLs = ['https://www.betexplorer.com/baseball/usa/mlb-2020/results/?stage=f3uUKla0&month=all',
        'https://www.betexplorer.com/baseball/usa/mlb-2021/results/?stage=vsOxxBj2&month=all',
        'https://www.betexplorer.com/baseball/usa/mlb-2022/results/?stage=MuHAcNJm&month=all',
        'https://www.betexplorer.com/baseball/usa/mlb-2023/results/?stage=4O099Es6&month=all',
        'https://www.betexplorer.com/baseball/usa/mlb/results/?month=all',
        'https://www.betexplorer.com/baseball/usa/mlb/fixtures/']

edge_options = Options()
# edge_options.add_argument("headless")
DRIVER = webdriver.Edge(options=edge_options)
DRIVER.maximize_window()

for URL in URLs[:-1]:
    DRIVER.get(URL)
    time.sleep(0.5)
    DRIVER.find_element(By.XPATH, '//*[@id="js-timezone"]/span').click()
    time.sleep(0.5)
    DRIVER.find_element(By.XPATH, "//button[@onclick=\"set_timezone('-5');\"]").click()
    time.sleep(0.5)
    html_content = DRIVER.page_source

    data_from_html = Pull_Hist_Odds_Data(html_content)
    df = pd.DataFrame(data_from_html, columns=["Match","Result","1","2","Date"])

    betting_data_df = pd.concat([betting_data_df, df])

betting_data_df = betting_data_df[betting_data_df["Date"].notna()].copy()
betting_data_df = Transform_DF(betting_data_df)
betting_data_df.replace('St.Louis Cardinals', 'St. Louis Cardinals', inplace=True)
betting_data_df.replace('Cleveland Indians', 'Cleveland Guardians', inplace=True)
betting_data_df.to_csv('hist_betting_data.csv', index=False)

DRIVER.get(URLs[-1])
time.sleep(0.5)
DRIVER.find_element(By.XPATH, '//*[@id="js-timezone"]/span').click()
time.sleep(0.5)
DRIVER.find_element(By.XPATH, "//button[@onclick=\"set_timezone('-5');\"]").click()
time.sleep(0.5)
html_content = DRIVER.page_source
data_from_today = Pull_Today_Odds_Data(html_content)
todays_odds = pd.DataFrame(data_from_today, columns=["game_date","Match","H_Payout","A_Payout"])

todays_odds[['home_name','away_name']] = todays_odds['Match'].str.split(' - ', expand=True)
todays_odds = todays_odds[(todays_odds['H_Payout'].notna()) | (todays_odds['A_Payout'].notna())][['game_date', 'home_name', 'away_name', 'H_Payout', 'A_Payout']].copy()

today = datetime.now()
max_idx = todays_odds[todays_odds['game_date'].str.contains('Today')].index.max() + 1
todays_odds = todays_odds[:max_idx].copy()
todays_odds['game_date'] = datetime.now().strftime('%Y-%m-%d')

last_valid_date_index = todays_odds['game_date'].last_valid_index()
todays_odds = todays_odds.loc[:last_valid_date_index]
todays_odds['game_date'].fillna(today.strftime('%Y-%m-%d'), inplace=True)

todays_odds['game_date'] = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))

todays_odds['H_Payout'] = pd.to_numeric(todays_odds['H_Payout'])
todays_odds['A_Payout'] = pd.to_numeric(todays_odds['A_Payout'])
todays_odds['H_odds'] = (1 / todays_odds['H_Payout'])
todays_odds['A_odds'] = (1 / todays_odds['A_Payout'])
todays_odds['vig'] = (todays_odds['H_odds'] + todays_odds['A_odds']) - 1
todays_odds['H_fair_odds'] = todays_odds['H_odds'] / (1 + todays_odds['vig'])
todays_odds['A_fair_odds'] = todays_odds['A_odds'] / (1 + todays_odds['vig'])

todays_odds.replace('St.Louis Cardinals', 'St. Louis Cardinals', inplace=True)
todays_odds.replace('Cleveland Indians', 'Cleveland Guardians', inplace=True)

todays_odds.to_csv('today_betting_data.csv', index=False)
DRIVER.quit()

#%%