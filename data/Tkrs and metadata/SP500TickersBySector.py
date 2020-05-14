import os
import urllib

import joblib
import quandl
from bs4 import BeautifulSoup

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.util.configuration_keys_util import getTempPath, getDatabasePath


def getSP500Tickers_cibersecurity():
    # Cyber    Security    ETF(NYSEARCA:HACK)
    sectors_symbols = ['IBM', 'RTN', 'MIME', 'CSCO', 'CYBR', 'SOPH', 'LMT', 'BAESY', 'RPD', 'PANW']
    return sectors_symbols


def getSP500Tickers_waterSupply():
    # http://eu.spindices.com/indices/equity/sp-global-water-index
    sectors_symbols = ['AWK', 'VIE', 'XYL', 'DHR', 'PNR', 'GEBN', 'IEX', 'UU', 'SEV', 'SVT', ]
    return sectors_symbols


def getSP500Tickers_virtualReality():
    # GOOGLE , facebook , gopro, nvidia,sony
    sectors_symbols = ['GOOGL', 'FB', 'GPRO', 'NVDA', 'SNE']
    return sectors_symbols


def getSP500Tickers_healthcareEquipment():
    sectors_symbols = getSP500TickersBySubIndustry()
    return sectors_symbols['health_care_equipment']


def getSP500Tickers_biotechnology():
    sectors_symbols = getSP500TickersBySubIndustry()
    return sectors_symbols['biotechnology']


def getSP500Tickers_realEstate():
    sectors_symbols = getSP500TickersBySector()
    return sectors_symbols['real_estate']


def getSP500Tickers_pharmaceuticals():
    sectors_symbols = getSP500TickersBySubIndustry()
    return sectors_symbols['pharmaceuticals']


def getSP500Tickers_financial():
    sectors_symbols = getSP500TickersBySector()
    return sectors_symbols['financials']


def getSP500Tickers_technology():
    sectors_symbols = getSP500TickersBySector()
    return sectors_symbols['information_technology']


def getSP500Tickers_healthcare():
    sectors_symbols = getSP500TickersBySector()
    return sectors_symbols['health_care']


def getSP500Tickers_industrials():
    sectors_symbols = getSP500TickersBySector()
    return sectors_symbols['industrials']


def getSP500Tickers_consumerStaples():
    sectors_symbols = getSP500TickersBySector()
    return sectors_symbols['consumer_staples']


def getSP500Tickers_energy():
    sectors_symbols = getSP500TickersBySector()
    return sectors_symbols['energy']


def getSP500TickersBySector():
    SITE = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # financials , industrials , energy ,health_care, information_technology, consumer_staples

    def scrape_list(site):
        hdr = {'User-Agent': 'Mozilla/5.0'}
        # req = urllib2.Request(site, headers=hdr)
        page = urllib.request.urlopen(site)
        soup = BeautifulSoup(page, features='lxml')

        table = soup.find('table', {'class': 'wikitable sortable'})
        sector_tickers = dict()
        for row in table.findAll('tr'):
            col = row.findAll('td')
            if len(col) > 0:
                sector = str(col[3].string.strip()).lower().replace(' ', '_')
                ticker = str(col[0].string.strip())
                if sector not in sector_tickers:
                    sector_tickers[sector] = list()
                sector_tickers[sector].append(ticker)
        return sector_tickers

    sector_tickers = scrape_list(SITE)
    print('possible sectors : %s' % sector_tickers.keys())
    return sector_tickers


def getSP500Tickers():
    bySector = getSP500TickersBySector()
    output = []
    for key in bySector.keys():
        output.append(bySector[key])
    return output[0]


def getSharadarDF(user_settings):
    quandl.ApiConfig.api_key = user_settings.quandl_token
    cacher = joblib.Memory(getTempPath(userSettings=user_settings))
    functionTemp = cacher.cache(quandl.get_table)
    return functionTemp("SHARADAR/TICKERS", paginate=True).drop_duplicates('ticker')


def getSharadarGroupsSector(user_settings, symbolList):
    data = getSharadarDF(user_settings)
    # ticke sector
    output = data[['ticker', 'sector']]
    # output = output[output['ticker'] == symbolList]
    output_dict = {}
    tickerList = output['ticker'].tolist()
    for symbol in symbolList:
        if symbol in tickerList:
            output_dict[symbol] = output['sector'][output['ticker'] == symbol].tolist()[0]
        else:
            output_dict[symbol] = 'na'
    return output_dict


def getSharadarTickers(user_settings):
    data = getSharadarDF(user_settings)
    return list(set(data['ticker'].tolist()))


def getSF0DF(user_settings):
    import pandas as pd
    metadata = getDatabasePath(user_settings) + os.sep + 'SF0_metadata.csv'
    df = pd.read_csv(metadata)
    return df


def getSF0GroupsSector(user_settings, symbolList):
    return getSharadarGroupsSector(user_settings, symbolList)


def getSF0TickersSelected(user_settings):
    from tradeasystems_connector.util.persist_util import read_text_file_list
    path = getDatabasePath(user_settings) + os.sep + 'SF0TickersSelected.txt'
    outputList = read_text_file_list(path)
    return outputList


def getSF0Tickers(user_settings):
    data = getSF0DF(user_settings)
    name = data['code'].str.split('_', expand=True)[0]
    output = list(set(name.tolist()))
    output.sort()
    return output


def getSP500TickersBySubIndustry():
    SITE = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # financials , industrials , energy ,health_care, information_technology, consumer_staples

    def scrape_list(site):
        hdr = {'User-Agent': 'Mozilla/5.0'}
        # urllib.request.urlopen(url)
        # req = urllib2.Request(site, headers=hdr)
        page = urllib.request.urlopen(site)
        soup = BeautifulSoup(page, features='lxml')

        table = soup.find('table', {'class': 'wikitable sortable'})
        sector_tickers = dict()
        for row in table.findAll('tr'):
            col = row.findAll('td')
            if len(col) > 0:
                sector = str(col[4].string.strip()).lower().replace(' ', '_')
                ticker = str(col[0].string.strip())
                if sector not in sector_tickers:
                    sector_tickers[sector] = list()
                sector_tickers[sector].append(ticker)
        return sector_tickers

    sector_tickers = scrape_list(SITE)
    print('possible subIndustries : %s' % sector_tickers.keys())
    return sector_tickers


def getIbexTickersBySector():
    SITE = "https://en.wikipedia.org/wiki/IBEX_35"

    # financials , industrials , energy ,health_care, information_technology, consumer_staples

    def scrape_list(site):
        hdr = {'User-Agent': 'Mozilla/5.0'}
        # urllib.request.urlopen(url)
        # req = urllib2.Request(site, headers=hdr)
        page = urllib.request.urlopen(site)
        soup = BeautifulSoup(page, features='lxml')

        table = soup.find('table', {'class': 'wikitable sortable'})
        sector_tickers = dict()
        for row in table.findAll('tr'):
            col = row.findAll('td')
            if len(col) > 0:
                sector = str(col[2].string.strip()).lower().replace(' ', '_')
                ticker = str(col[1].string.strip())
                if sector not in sector_tickers:
                    sector_tickers[sector] = list()
                sector_tickers[sector].append(ticker)
        return sector_tickers

    sector_tickers = scrape_list(SITE)
    print('possible subIndustries : %s' % sector_tickers.keys())
    return sector_tickers


def getIbexTickers():
    import urllib.request
    from bs4 import BeautifulSoup
    import re

    url = 'https://en.wikipedia.org/wiki/IBEX_35'

    # %%
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass

        return False

    # %%
    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page, "lxml")

    res = [y.text for y in soup.findAll('td')]
    # %%
    res1 = []
    for row in res:
        containBreaks = u"\n" in row
        containNumbers = is_number(row)
        containSpaces = len(row.split()) > 1
        containLetters = re.search('[a-zA-Z]', row) is not None
        longerThan = len(row) > 7
        shorterThan = len(row) < 3
        containMC = row.endswith('.MC')
        if not containBreaks and not containNumbers and not containSpaces and containLetters and not longerThan and not shorterThan and containMC:
            # row = row.replace('.', '-')
            # name = str(row) + '.MC'
            if row not in res1:
                res1.append(row)
    # res1.remove('AENA.MC')
    # res1.remove('CLNX.MC')
    # res1.remove('SGRE.MC')
    # res1.remove('MRL.MC')
    return res1


def getAllYahooTickers(user_settings):
    temp_dir = getTempPath(userSettings=user_settings)
    cacher = joblib.Memory(temp_dir)
    functionTemp = cacher.cache(__readExcelYahooTickers__)
    filename = getDatabasePath(user_settings) + os.sep + 'yahoo_ticker_09_2017.xlsx'
    df = functionTemp(filename, sheet_name='Stock')
    return df['Ticker'].tolist()


def __readExcelYahooTickers__(filename, sheet_name=0):
    import pandas as pd
    df = pd.read_excel(filename, sheet_name=sheet_name, headers=4)
    df.columns = df.iloc[2]
    df.drop([0, 1, 2], axis=0, inplace=True)

    return df


def getSP500Tickers_manual():
    symbols = ['AAPL', 'ABT', 'ACN', 'ADT', 'AAP', 'AES', 'AET', 'AFL',
               'AMG', 'A', 'ARE', 'APD', 'AKAM', 'AGN', 'ALXN', 'ALLE', 'ADS', 'ALL', 'MO', 'AMZN',
               'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'ABC', 'AME', 'AMGN', 'APH', 'APC', 'ADI', 'AON', 'APA',
               'AIV', 'AMAT', 'ADM', 'AIZ', 'T', 'ADSK', 'ADP', 'AN', 'AZO', 'AVGO', 'AVB', 'AVY', 'BHI', 'BLL', 'BAC',
               'BK', 'BCR', 'BAX', 'BBT', 'BDX', 'BBBY', 'BBY', 'HRB', 'BA', 'BWA', 'BXP',
               'BMY', 'BRCM', 'CHRW', 'CA', 'CVC', 'COG', 'CPB', 'COF', 'CAH', 'HSIC', 'KMX',
               'CCL', 'CAT', 'CBG', 'CBS', 'CELG', 'CNP', 'CTL', 'CERN', 'CF', 'SCHW', 'CHK', 'CVX', 'CMG', 'CB', 'CI',
               'XEC', 'CINF', 'CTAS', 'CSCO', 'C', 'CTXS', 'CLX', 'CME', 'CMS', 'COH', 'KO', 'CTSH', 'CL',
               'CMCSA', 'CMA', 'CSC', 'CAG', 'COP', 'CNX', 'ED', 'STZ', 'GLW', 'COST', 'CCI', 'CSX', 'CMI', 'CVS',
               'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DLPH', 'DAL', 'XRAY', 'DVN', 'DO', 'DFS', 'DISCA',
               'DLTR', 'D', 'DOV', 'DOW', 'DPS', 'DTE', 'DD', 'DUK', 'DNB', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL',
               'EIX', 'EW', 'EA', 'EMC', 'EMR', 'ESV', 'ETR', 'EOG', 'EQT', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL',
               'ES', 'EXC', 'EXPE', 'EXPD', 'ESRX', 'XOM', 'FFIV', 'FB', 'FAST', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE',
               'FISV', 'FLIR', 'FLS', 'FLR', 'FMC', 'FTI', 'F', 'FOSL', 'BEN', 'FCX', 'FTR', 'GME', 'GPS', 'GRMN', 'GD',
               'GE', 'GGP', 'GIS', 'GM', 'GPC', 'GNW', 'GILD', 'GS', 'GT', 'GOOGL', 'GWW', 'HAL', 'HBI', 'HOG',
               'HAR', 'HRS', 'HIG', 'HAS', 'HCA', 'HCP', 'HCN', 'HP', 'HES', 'HPQ', 'HD', 'HON', 'HRL', 'HST',
               'HUM', 'HBAN', 'ITW', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ',
               'IRM', 'JBHT', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'GMCR', 'KMB', 'KIM', 'KMI',
               'KLAC', 'KSS', 'KR', 'LB', 'LLL', 'LH', 'LRCX', 'LM', 'LEG', 'LEN', 'LVLT', 'LUK', 'LLY', 'LNC',
               'LLTC', 'L', 'LYB', 'MAC', 'M', 'MNK', 'MRO', 'MPC', 'MAR', 'MMC', 'MLM',
               'MA', 'MAT', 'MKC', 'MCD', 'MCK', 'MJN', 'MRK', 'MET', 'KORS', 'MCHP', 'MSFT', 'MHK',
               'TAP', 'MDLZ', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MUR', 'MYL', 'NDAQ', 'NOV',
               'NFLX', 'NFX', 'NEM', 'NWSA', 'NEE', 'NLSN', 'NKE', 'NI', 'NE', 'JWN', 'NSC', 'NTRS',
               'NOC', 'NRG', 'NUE', 'NVDA', 'ORLY', 'OXY', 'OMC', 'OKE', 'ORCL', 'OI', 'PCAR', 'PH', 'PDCO',
               'PAYX', 'PNR', 'PBCT', 'POM', 'PEP', 'PKI', 'PRGO', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PXD', 'PBI',
               'PCL', 'PNC', 'RL', 'PPG', 'PPL', 'PX', 'PCP', 'PCLN', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA',
               'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RRC', 'RTN', 'O', 'RHT', 'REGN', 'RF', 'RSG', 'RAI', 'RHI',
               'ROK', 'ROP', 'ROST', 'R', 'CRM', 'SNDK', 'SCG', 'SLB', 'SNI', 'STX', 'SEE', 'SRE', 'SHW',
               'SPG', 'SLG', 'SJM', 'SNA', 'SO', 'LUV', 'SWN', 'SE', 'STJ', 'SWK', 'SPLS', 'SBUX', 'HOT', 'STT',
               'SRCL', 'SYK', 'STI', 'SYMC', 'SYY', 'TROW', 'TGT', 'TE', 'TGNA', 'THC', 'TDC', 'TSO', 'TXN',
               'TXT', 'HSY', 'TRV', 'TMO', 'TIF', 'TWX', 'TWC', 'TJX', 'TMK', 'TSS', 'TSCO', 'RIG', 'TRIP',
               'TSN', 'UA', 'UNP', 'UNH', 'UPS', 'URI', 'UTX', 'UHS', 'UNM', 'URBN', 'VFC', 'VLO', 'VTR',
               'VRSN', 'VZ', 'VRTX', 'VIAB', 'V', 'VNO', 'VMC', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'ANTM', 'WFC', 'WDC',
               'WU', 'WY', 'WHR', 'WFM', 'WMB', 'WEC', 'WYN', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XL', 'XYL', 'YUM',
               'ZBH', 'ZION', 'ZTS']
    return symbols


def getInstrumentList(symbolList, asset_type, currency=Currency.eur, broker=None):
    instrumentList = []
    for ticker in symbolList:
        if asset_type == AssetType.us_equity:
            currency = Currency.usd

        instrument = Instrument(ticker, asset_type=asset_type, broker=broker, currency=currency)
        instrumentList.append(instrument)
    return instrumentList
