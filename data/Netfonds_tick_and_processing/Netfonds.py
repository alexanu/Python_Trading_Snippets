

import requests as request
from bs4 import BeautifulSoup as bs
import re
from pprint import pprint
import sys
from datetime import datetime
import time
import json


class Info(object):
    def __init__(self, instrument, exchange='OSE'):

        html = request.get('http://www.netfonds.no/quotes/about.php?paper=%s.%s' % (instrument, exchange))
        s = bs(html.text, "lxml")

        table = s.find(id='updatetable1').find_all('tr')

        structure = {1: 'instrument',
                     2: 'name',
                     3: 'exchange',
                     4: 'type',
                     5: 'active',
                     6: 'tradable',
                     7: 'index1',
                     8: 'index2',
                     9: 'index3',
                     10: 'prev_tradeday',
                     11: 'prev_quote',
                     12: 'quantity',
                     13: 'mcap',
                     14: 'lot_size',
                     15: 'min_amount',
                     16: 'ISIN',
                     17: 'currency',
                     18: 'about'}

        self.info = {}
        index3 = False

        for key, row in enumerate(table):

            if key == 0:
                continue

            if key == 9:
                for cell in row.find_all('th'):
                    if cell.get_text().strip() == 'Indeks':
                        index3 = True
            skey = key
            for cell in row.find_all('td'):

                if skey >= 9 and not index3:
                    skey = skey + 1

                if skey in [5, 6]:
                    value = cell.get_text().strip()
                    value = True if value == 'Ja' else False
                elif skey == 10:
                    value = datetime.strptime(cell.get_text().strip(), '%d/%m-%Y').date()
                elif skey == 11:
                    value = float(cell.get_text().strip())
                elif skey in [12, 13, 14]:
                    value = int(cell.get_text().replace(' ', ''))
                elif skey == 16:
                    value = cell.get_text().replace(' ', '')
                else:
                    value = cell.get_text().strip()

                self.info.update({structure[skey]: value})

        # Assign dictionary to properties
        for k, v in self.info.items():
            setattr(self, k, v)

    def get_info(self):
        return self.info

    def is_active(self):
        return self.info.get('active', False)

    def is_tradable(self):
        return self.info.get('tradable', False)

    def get_indexes(self):

        return {1: self.info.get('index1', None),
                2: self.info.get('index2', None),
                3: self.info.get('index3', None)}
