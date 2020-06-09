# Source https://github.com/alexanu/stock-split-calendar



import requests
from bs4 import BeautifulSoup

class Record:
    def __init__(self, name, symbol, country, factor, date):
        self.name = name
        self.symbol = symbol
        self.country = country
        self.factor = factor
        self.date = date

    def __str__(self):
        return self.name + \
               ' ' * (40 - len(self.name)) + \
               self.symbol + \
               ' ' * (20 - len(self.symbol)) + \
               self.country + \
               ' ' * (20 - len(self.country)) + \
               self.factor + \
               ' ' * (20 - len(self.factor)) + \
               self.date


url = 'https://www.investing.com/stock-split-calendar/Service/getCalendarFilteredData'


def get_stock_split_calendar(date='2019-04-10', debug=True):
    start_date = '2019-04-10'
    end_date = date  # important for the script to work well. We query only one day.
    data = 'country%5B%5D=29&country%5B%5D=25&country%5B%5D=54&country%5B%5D=145&country%5B%5D=34&country%5B%5D=174&country%5B%5D=163&country%5B%5D=32&country%5B%5D=70&country%5B%5D=6&country%5B%5D=27&country%5B%5D=37&country%5B%5D=122&country%5B%5D=15&country%5B%5D=113&country%5B%5D=107&country%5B%5D=55&country%5B%5D=24&country%5B%5D=59&country%5B%5D=71&country%5B%5D=22&country%5B%5D=17&country%5B%5D=51&country%5B%5D=39&country%5B%5D=93&country%5B%5D=106&country%5B%5D=14&country%5B%5D=48&country%5B%5D=33&country%5B%5D=23&country%5B%5D=10&country%5B%5D=35&country%5B%5D=92&country%5B%5D=57&country%5B%5D=94&country%5B%5D=68&country%5B%5D=103&country%5B%5D=42&country%5B%5D=109&country%5B%5D=188&country%5B%5D=7&country%5B%5D=105&country%5B%5D=172&country%5B%5D=21&country%5B%5D=43&country%5B%5D=20&country%5B%5D=60&country%5B%5D=87&country%5B%5D=44&country%5B%5D=193&country%5B%5D=125&country%5B%5D=45&country%5B%5D=53&country%5B%5D=38&country%5B%5D=170&country%5B%5D=100&country%5B%5D=56&country%5B%5D=52&country%5B%5D=238&country%5B%5D=36&country%5B%5D=90&country%5B%5D=112&country%5B%5D=110&country%5B%5D=11&country%5B%5D=26&country%5B%5D=162&country%5B%5D=9&country%5B%5D=12&country%5B%5D=46&country%5B%5D=41&country%5B%5D=202&country%5B%5D=63&country%5B%5D=123&country%5B%5D=61&country%5B%5D=143&country%5B%5D=4&country%5B%5D=5&country%5B%5D=138&country%5B%5D=178&country%5B%5D=75&dateFrom={0}&dateTo={1}&limit_from=0'.format(
        date, end_date)
    # data = 'country%5B%5D=29&country%5B%5D=25&country%5B%5D=54&country%5B%5D=145&country%5B%5D=34&country%5B%5D=174&country%5B%5D=163&country%5B%5D=32&country%5B%5D=70&country%5B%5D=6&country%5B%5D=27&country%5B%5D=37&country%5B%5D=122&country%5B%5D=15&country%5B%5D=113&country%5B%5D=107&country%5B%5D=55&country%5B%5D=24&country%5B%5D=59&country%5B%5D=71&country%5B%5D=22&country%5B%5D=17&country%5B%5D=51&country%5B%5D=39&country%5B%5D=93&country%5B%5D=106&country%5B%5D=14&country%5B%5D=48&country%5B%5D=33&country%5B%5D=23&country%5B%5D=10&country%5B%5D=35&country%5B%5D=92&country%5B%5D=57&country%5B%5D=94&country%5B%5D=68&country%5B%5D=103&country%5B%5D=42&country%5B%5D=109&country%5B%5D=188&country%5B%5D=7&country%5B%5D=105&country%5B%5D=172&country%5B%5D=21&country%5B%5D=43&country%5B%5D=20&country%5B%5D=60&country%5B%5D=87&country%5B%5D=44&country%5B%5D=193&country%5B%5D=125&country%5B%5D=45&country%5B%5D=53&country%5B%5D=38&country%5B%5D=170&country%5B%5D=100&country%5B%5D=56&country%5B%5D=52&country%5B%5D=238&country%5B%5D=36&country%5B%5D=90&country%5B%5D=112&country%5B%5D=110&country%5B%5D=11&country%5B%5D=26&country%5B%5D=162&country%5B%5D=9&country%5B%5D=12&country%5B%5D=46&country%5B%5D=41&country%5B%5D=202&country%5B%5D=63&country%5B%5D=123&country%5B%5D=61&country%5B%5D=143&country%5B%5D=4&country%5B%5D=5&country%5B%5D=138&country%5B%5D=178&country%5B%5D=75&dateFrom=2017-10-12&dateTo=2017-10-20&limit_from=0'

    headers = {'Host': 'www.investing.com',
               'Origin': 'https://www.investing.com',
               'Referer': 'https://www.investing.com/stock-split-calendar/',
               'X-Requested-With': 'XMLHttpRequest',
               'Content-Type': 'application/x-www-form-urlencoded',
               'Connection': 'keep-alive',
               'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8,ja;q=0.7,es;q=0.6',
               'Accept-Encoding': 'Encoding:gzip, deflate, br',
               'Accept': '*/*',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36'}

    content = requests.post(url, data=data, headers=headers).content

    if 'No data available for the dates specified' in content.decode('utf8'):
        return []

    soup = BeautifulSoup(content, 'lxml')
    # print(soup.find_all('tr')[11].find_all('td'))

    # [(tr.find_all('td')[1].contents[1].contents[1].contents[0], tr.find_all('td')[1].contents[1].contents[1].contents[1], str(tr.find_all('td')[2].contents[0].strip().replace('\\n', '').replace('\\t', ''))) for tr in soup.find_all('tr')]

    records = []
    for i, tr in enumerate(soup.find_all('tr')):
        country = list(set(tr.find_all('td')[1].contents[1].attrs.keys()) - set(['class', 'title', 'middle']))[0]
        tra = tr.find_all('td')[1].contents[1].contents[1]
        company_name = tra.contents[0][0:-1].strip()
        tmp_symbol = str(tra.contents[1].contents[0])
        company_symbol = tmp_symbol[0:tmp_symbol.rindex(')')]

        tmp_factor = tr.find_all('td')[2].contents[0]
        if ',' in tmp_factor:
            tmp_factor = tmp_factor[:tmp_factor.index(',') - 1]
        factor = str(tmp_factor.strip().replace('\\n', '').replace('\\t', ''))

        record = Record(name=company_name,
                        symbol=company_symbol,
                        country=country,
                        factor=factor,
                        date=date)
        # if debug:
        #     print(record)
        records.append(record)
    return records


if __name__ == '__main__':
    get_stock_split_calendar()