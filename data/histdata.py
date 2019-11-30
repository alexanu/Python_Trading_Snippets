import os
import sys
from datetime import datetime, timedelta
import logging
import re

import bs4 as bs
import h5py
import pandas as pd
import requests
from tqdm import tqdm
import zipfile

logger = logging.getLogger(__name__)

HISTDATA_URL = 'https://www.histdata.com/'


def date_formatter(raw_string):
    """Return a well formatted date as string."""
    date = raw_string[:8]
    date = f'{date[:4]}-{date[4:6]}-{date[6:8]}'
    time = raw_string[9:]
    if len(raw_string) > 15:
        time = f'{time[0:2]}:{time[2:4]}:{time[4:6]}.{time[6:]}'
    else:
        time = f'{time[0:2]}:{time[2:4]}:{time[4:6]}'
    return ' '.join((date, time))


class DataSet:
    """TickSet object.
    Each set to download from histdata's website is represented as an
    object. The set can simply be downloaded by calling the "get" method.
    :args::
        instrument (str): instrument that you want to get the data example
                          "EURUSD"
        year (int) : the year that you want to get the data e.g. 2019
        month (int) : the month that you want to get the data e.g. 6
    :example::
        >>> tick_set = TickSet(EURUSD', 2019, 1, type='M1') #Jan 2019, 1 minute
        >>> tick_set.get()
    """
    HISTDATA_URL = HISTDATA_URL
    data_url = f'{HISTDATA_URL}download-free-forex-historical-data/?/ascii'
    ticks_url = f'{data_url}/tick-data-quotes'
    ohlc_url = f'{data_url}/1-minute-bar-quotes'
    post_url = f'{HISTDATA_URL}get.php'

    def __init__(self, instrument, year, month, type='M1', tmp_dir="/tmp"):
        logger.debug("tick set object init with args : %s | %i | %i",
                     instrument, year, month)
        self._type = None
        self._tmp_dir = tmp_dir
        self._zip_name = None
        self._csv_path = None
        self.instrument = instrument
        self.year = year
        self.month = month
        self.type = type

    @property
    def url(self):
        """Return url dynamically using provided informations."""
        if self.type.upper() == 'M1':
            data_url = self.ohlc_url
        elif self.type.lower() == 'ticks':
            data_url = self.ticks_url
        return f'{data_url}/{self.instrument}/{self.year}/{self.month}'

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        if type.lower() in ['m1', 'ticks'] or type.upper() in ['M1', 'TICKS']:
            self._type = type
        else:
            raise ValueError('Type must be M1 or ticks')

    @staticmethod
    def _header(referer=None):
        header = {"User-Agent": "Mozilla/5.0",
                  "Accept-Encoding": None,
                  "Referer": referer}
        return header

    def __str__(self):
        return f"{self.instrument}/y{self.year}/m{self.month}"

    def _download(self, session=None, tqdm_support=True):
        logger.debug("Start download.")
        if not session:
            session = requests.Session()
        response = session.get(self.url)
        form_data = self._get_form_to_send(response)
        response = session.post(self.post_url, form_data, stream=True,
                                headers=self._header(referer=self.url))
        content_disposition = response.headers.get("content-disposition")
        content_length = int(response.headers.get("content-length"))
        self._zip_name = str(content_disposition).split("=")[-1]

        with open(os.path.join(self._tmp_dir, self._zip_name), 'wb') as z_file:
            chunk_size = 1024
            for data in tqdm(response.iter_content(chunk_size=chunk_size),
                             total= content_length // chunk_size,
                             desc='Download set %s' % self.__str__(),
                             ascii=True if not tqdm_support else False,
                             file=sys.stdout):
                z_file.write(data)

        logger.debug("Download complete")
        session.close()

    def _get_form_to_send(self, response):
        soup = bs.BeautifulSoup(response.content, 'lxml')
        form = soup.find_all('form')[0]
        data_to_send = dict()
        for field in form.find_all('input'):
            data_to_send[field.attrs['name']] = field.attrs['value']
        return data_to_send

    def _extract(self, purge=True):
        """This will extract the files from the zip archive."""
        logger.debug("Extracting set : %s", self.__str__())
        path = os.path.join(self._tmp_dir, self._zip_name)
        with zipfile.ZipFile(path, 'r') as zip_r:
            output_path = str(path).split('.')[0]
            zip_r.extractall(output_path)
        csv_name = [x for x in os.listdir(output_path) if x.endswith(".csv")]
        assert len(csv_name) == 1
        self._csv_path = os.path.join(output_path, csv_name[0])
        if purge:
            logger.debug("Removing %s", self._zip_name)
            os.remove(os.path.join(self._tmp_dir, self._zip_name))
        logger.debug("Extraction complete")

    def _load_csv(self):
        """Load data from the CSV and return it as a pandas.DataFrame."""
        logger.debug("Loading csv.")
        if self.type.upper() == 'M1':
            names = ['timestamp', 'open', 'high', 'low', 'close', 'vol']
        elif self.type.lower() == 'ticks':
            names = ["timestamp", "bid", "ask", "vol"]
        else:
            raise ValueError('Invalid type data')
        df = pd.read_csv(self._csv_path,
                         names=names,
                         sep=';' if self.type.upper() == 'M1' else ',')
        df['timestamp'] = df['timestamp'].apply(date_formatter)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    def _save(self, path_hdf):
        """This will save the dataframe in a hdf5 file."""
        logger.debug("Saving data in hdf5 file.")
        df = self._load_csv()
        df.to_hdf(path_hdf, self.__str__(), table=True, mode="a")
        path = "/".join(self._csv_path.split("/")[:-1])

    def get(self, session=None, output_path=None, tqdm_support=False):
        """Get method. Run this for process a set."""
        logger.debug("get set : %s ", self.__str__())
        self._download(session, tqdm_support=tqdm_support)
        self._extract()
        if not output_path:
            raise Exception('No output_path')
        self._save(output_path)

    @property
    def should_update(self):
        today = datetime.today()
        date_set = datetime(year=self.year, month=self.month, day=1)
        if (today - date_set) > timedelta(days=30):
            return False
        return True

class SetDownloader:
    HISTDATA_URL = HISTDATA_URL

    def __init__(self, config):
        logger.debug("SetDownloader called with args: %s", config)
        self.session = self._init_session()
        self.instruments = config.get('instruments')
        self.date_start = config.get('date_start')
        self.date_end = config.get('date_end')
        self.output_path = config.get('output_path')
        self.type = config.get('type')
        self.tqdm_support = config.get('tqdm')

    def _init_session(self):
        session = requests.Session()
        try:
            response = session.get(self.HISTDATA_URL)
            if response.status_code != 200:
                raise ConnectionError('HTTP error %i', response.status_code)
        except requests.exceptions.RequestsException as e:
            logger.exception(e)
            raise ConnectionError("Histdata's website is unreachable. "
                                  "Check your internet connection.")
        return session

    @property
    def all_sets(self):
        for instrument in self.instruments:
            for date in pd.date_range(self.date_start, self.date_end,
                                      freq='M', closed='left'):
                yield DataSet(instrument, date.year, date.month,
                              type=self.type)

    def run(self):
        logger.debug('Run method called.')
        try:
            index = self._get_index()
        except OSError:
            logger.info('No index')
            index = []
        for dataset in self.all_sets:
            if str(dataset) in index:
                logger.info('Skipping %s, already in dataset.', str(dataset))
            else:
                logger.info("Processing set : %s", str(dataset))
                dataset.get(session=self.session, output_path=self.output_path,
                            tqdm_support=self.tqdm_support)
                logger.info("Done. Set : %s [saved]", dataset)
        self.session.close()

    def _get_index(self):
        logger.debug("Get index")
        index = []
        hdf5_file = h5py.File(self.output_path, 'r')
        for instr in self._get_available_instruments(hdf5_file):
            for year in self._get_available_years_for_instr(hdf5_file, instr):
                for month in self._get_available_months(hdf5_file, instr, year):
                    index.append(instr + '/' + year + '/' + month)
        hdf5_file.close()
        logger.debug('Index : %s ', index)
        return index

    def _get_available_instruments(self, hdf5_file):
        return list(hdf5_file.keys())

    def _get_available_years_for_instr(self, hdf5_file, instrument):
        return list(hdf5_file[instrument].keys())

    def _get_available_months(self, hdf5_file, instrument, year):
        return list(hdf5_file[instrument + '/' + year])


def load_available_pairs(url='https://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes'):
    logger.debug("Loading available pairs from histdata")
    response = requests.get(url)
    soup = bs.BeautifulSoup(response.content, 'html.parser')
    table = soup.find_all('table')[0]
    instr = table.find_all('strong')
    instr = list(map(str, instr))
    regex = re.compile(r'[A-Z]{3}/[A-Z]{3}')
    parser = lambda x : ''.join(regex.findall(x)[0].split('/'))
    return list(map(parser, instr))
