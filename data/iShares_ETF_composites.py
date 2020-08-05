# Source: https://github.com/AHinterding/etf-loader

import csv
import datetime as dt
from pathlib import Path
from functools import lru_cache
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
import plotly.graph_objects as go

country = "us"
file_path = Path('data') / f'product_screener_{country}.csv'
us_frame = pd.read_csv(file_path)
download_date = dt.date.today()
for product_nr, product_info in us_frame.iterrows():
    ticker = product_info['Ticker']
    url = product_info['URL']
    self.logger.info(f'Downloading compo for {ticker} from {url}!')

    response = requests.get(url)
    single_etf_soup = BeautifulSoup(response.content, 'html.parser')
    actual_link = None
    for link in single_etf_soup.find_all('a'):
        try:
            current_link = link.get('href')
            if 'csv' in current_link:
                actual_link = current_link
                break
        except Exception as e:
            self.logger.warning(e)
    composition_link = f"https://www.ishares.com{actual_link}" if actual_link else None    
    if composition_link:
        try:
            with requests.Session() as s:
                download = s.get(composition_link)
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                my_list = list(cr)
                composition = pd.DataFrame(my_list[10:-1], columns=my_list[9])

            export_path = Path('downloads') / 'compositions' / f'{download_date}'
            if not export_path.exists():
                export_path.mkdir(parents=True)

            composition_file_name = f'{ticker}_holdings_{download_date}.csv'
            composition.to_csv(export_path / composition_file_name)
        except ValueError:
            warning_msg = f'Invalid downloaded file for {ticker} from {url}, no data downloaded!'
            self.logger.warning(warning_msg)
    else:
        warning_msg = f'No composition found for {ticker} at {url}!'
        self.logger.warning(warning_msg)



    def get_country_weights(self, file_path: Path) -> pd.DataFrame:
        compo_frame = pd.read_csv(file_path, index_col=0)
        compo_frame = compo_frame[compo_frame['Asset Class'] == 'Equity'].copy()
        compo_frame['Weight (%)'] = compo_frame['Weight (%)'].apply(lambda x: float(x))
        compo_frame['iso2_code'] = compo_frame['ISIN'].apply(lambda x: x[:2])
        compo_frame['iso3_code'] = compo_frame['iso2_code'].apply(lambda x: self.get_iso3_from_iso2(x))
        compo_frame.dropna(inplace=True)
        compo_frame_grouped = compo_frame.groupby('iso3_code').sum()

        file_path = Path('data') / 'iso_country_mapping.csv'
        iso_frame = pd.read_csv(file_path)
        iso_frame['Alpha-2 code'] = iso_frame['Alpha-2 code'].apply(lambda x: x.replace(' ', '')).copy()
        iso_frame['Alpha-3 code'] = iso_frame['Alpha-3 code'].apply(lambda x: x.replace(' ', '')).copy()

        country_weights = pd.DataFrame(data=iso_frame['Alpha-3 code'].values,
                                       index=iso_frame['Alpha-3 code'],
                                       columns=['iso3_code'])
        country_weights['weight'] = 0

        for iso3 in compo_frame_grouped.index:
            country_weights.loc[iso3, 'weight'] = compo_frame_grouped.loc[iso3, 'Weight (%)']

        country_weights.reset_index(inplace=True, drop=True)
        country_weights['country'] = country_weights['iso3_code'].apply(lambda x: self.get_country_name_from_iso3(x))
        country_weights.loc[:, 'weight log'] = country_weights['weight'].apply(lambda x: np.log(x))
        return country_weights

    def get_country_name_from_iso3(self, iso3: str) -> str:
        iso_frame = self.load_iso_mapping()
        if iso3 in iso_frame['Alpha-3 code'].values:
            target_idx = list(iso_frame['Alpha-3 code']).index(iso3)
            country = iso_frame['Name'][target_idx]
        else:
            country = None
        return country

    def get_iso3_from_iso2(self, iso2: str) -> str:
        iso_frame = self.load_iso_mapping()
        if iso2 in iso_frame['Alpha-2 code'].values:
            target_idx = list(iso_frame['Alpha-2 code']).index(iso2)
            iso3_str = iso_frame['Alpha-3 code'][target_idx]
        else:
            iso3_str = None
        return iso3_str
