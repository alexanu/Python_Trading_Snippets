import itertools
import logging
import sys
import os
import re


def default_data_path():
    match = re.match(r'.+/forex-lab', os.getcwd())
    if match:
        return os.path.join(match.group(), 'data')
    return os.path.join(os.getcwd(), 'data')


DATA_PATH = default_data_path()
CURRENCIES = ['eur', 'gbp', 'aud', 'nzd', 'usd', 'cad', 'chf', 'jpy']


def pairs():
    return list(itertools.combinations(CURRENCIES, 2))


def configure_logs(level=logging.INFO):
    logging.basicConfig(stream=sys.stdout, level=level, format='%(levelname)s: %(message)s')
