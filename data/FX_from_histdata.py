import requests
import re
import os
import zipfile
import logging
import hashlib
import itertools
import logging
import sys



HIST_DATA_URL = 'http://www.histdata.com/download-free-forex-historical-data/?/ascii/1-minute-bar-quotes/{}/{}'
CSV_FILE = 'DAT_ASCII_{}_M1_{}.csv'


def pair_string(pair):
    return ''.join(pair).lower()

def default_data_path():
    match = re.match(r'.+/forex-lab', os.getcwd())
    if match:
        return os.path.join(match.group(), 'data')
    return os.path.join(os.getcwd(), 'data')

DATA_PATH = default_data_path()

def create_data_directory(pair):
    output_path = os.path.join(DATA_PATH, pair_string(pair))
    if not os.path.exists(output_path):
        os.makedirs(output_path)


        

def year_month(year, month=None, sep=''):
    if month is None:
        return str(year)
    return '{}{}{:02d}'.format(year, sep, month)        
        
def get_archive_path(pair, year, month=None):
    return os.path.join(DATA_PATH, pair_string(pair), '{}.zip'.format(year_month(year, month)))


def download_hist_data(pair, year, month=None):
    """Downloads historical data from histdata.com given the pair, year and month (optional)."""

    logging.info('Downloading %s data for %s %s', pair, year, month)
    url = HIST_DATA_URL.format(pair_string(pair), year_month(year, month, sep='/'))
    response = requests.get(url)
    assert response.status_code == 200
    match_tk = re.search(r'id="tk" value="(.{32})"', str(response.content))
    assert match_tk
    data = {'tk': match_tk.group(1),
            'date': str(year),
            'datemonth': year_month(year, month),
            'platform': 'ASCII',
            'timeframe': 'M1',
            'fxpair': pair_string(pair)}
    response = requests.post(url='http://www.histdata.com/get.php', data=data, headers={'Referer': url})
    assert response.status_code == 200

    with open(get_archive_path(pair, year, month), 'wb') as file:
        file.write(response.content)


def get_prices(pair, year, month=None):
    """Retrieves the historical data given the pair, year and month (optional)."""

    create_data_directory(pair)
    archive_path = get_archive_path(pair, year, month)
    if os.path.exists(archive_path):
        logging.info('%s already exists, skipping download!', archive_path)
    else:
        download_hist_data(pair, year, month)

    csv_file = CSV_FILE.format(pair_string(pair).upper(), year_month(year, month))
    lines = zipfile.ZipFile(archive_path).open(csv_file).readlines()
    lines_utf8 = map(lambda l: l.decode('utf8').strip(), lines)
    lines_filtered = filter(lambda l: l.startswith(str(year)), lines_utf8)
    return list(lines_filtered)


CURRENCIES = ['eur', 'gbp', 'aud', 'nzd', 'usd', 'cad', 'chf', 'jpy']
def pairs():
    return list(itertools.combinations(CURRENCIES, 2))

def merge_all_pairs(year, month=None):
    """Merges the prices of all pairs into one entry per timestamp."""

    pairs = pairs()
    all_prices_dict = {}
    for i in range(len(pairs)):
        price_entries = get_prices(pairs[i], year, month)
        for entry in price_entries:
            parts = entry.split(';')
            timestamp = parts[0]
            close_price = parts[-2]
            if timestamp not in all_prices_dict:
                all_prices_dict[timestamp] = [None] * len(pairs)
            all_prices_dict[timestamp][i] = close_price

    timestamps = list(all_prices_dict.keys())
    timestamps.sort()
    return [[timestamp] + all_prices_dict[timestamp] for timestamp in timestamps]


def fill_gaps(entries):
    for i in range(1, len(entries)):
        for j in range(1, len(entries[i])):
            if entries[i][j] is None:
                entries[i][j] = entries[i - 1][j]


def merge_all_pairs_to_file(file_path, year, month=None):
    """Writes the merged prices of all pairs to file given a year and month (optional)."""

    merged_entries = merge_all_pairs(year, month)
    fill_gaps(merged_entries)
    logging.info('Adding merged pair prices for %s %s to %s', year, month, file_path)
    with open(file_path, 'a') as file:
        for entry in merged_entries:
            if None not in entry:
                file.write(';'.join(entry))
                file.write(os.linesep)


def prepare_data(from_year, to_year, to_month=None):
    """Writes the merged prices of all pairs to file given a date range."""

    currencies_hash = hashlib.md5(','.join(cfg.CURRENCIES).encode('utf-8')).hexdigest()
    out_dir = os.path.join(DATA_PATH, currencies_hash)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    out_file = os.path.join(out_dir, '{}_{}'.format(year_month(from_year), year_month(to_year, to_month)))
    if os.path.exists(out_file):
        logging.info('%s already exists, skipping generation!', out_file)
        return out_file

    for year in range(from_year, to_year):
        merge_all_pairs_to_file(out_file, year)
    if to_month is not None:
        for month in range(1, to_month):
            merge_all_pairs_to_file(out_file, to_year, month)

    return out_file


def configure_logs(level=logging.INFO):
logging.basicConfig(stream=sys.stdout, level=level, format='%(levelname)s: %(message)s')

if __name__ == "__main__":
    cfg.configure_logs()
    prepare_data(2015, 2019)
