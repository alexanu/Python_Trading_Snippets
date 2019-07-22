from datetime import datetime
from datetime import timedelta
import requests
from pymongo import MongoClient

client = MongoClient()
db = client["finviz"]


class FinvizStore(object):

    def __init__(self):
        self.stocks = db["stocks"]
        self.keystats = db["keystats"]

    def find(self, symbol):
        # Refresh data when last update is older more 1 day.
        gt_date = datetime.utcnow() - timedelta(days=1)
        # Query
        q_recent_record = {
            "symbol": symbol,
            "update_date": {"$gt": gt_date}
        }
        res = self.stocks.find(q_recent_record)
        return res

    def create_stock(self, stock):
        try:
            res = self.stocks.insert_one(stock)
            stock["_id"] = str(res.inserted_id)
            return stock
        except Exception as e:
            print(e)

    def create_keystats(self, stats, symbol):
        try:
            stats["symbol"] = symbol.upper()
            stats["create_date"] = datetime.utcnow()
            stats["update_date"] = datetime.utcnow()
            res = self.keystats.insert_one(stats)
        except Exception as e:
            print(e)



from html.parser import HTMLParser


class FinvizKeyStatsParser(HTMLParser):

    def __init__(self):
        super().__init__()
        self.datamap = {}
        self.last_indicator = None
        self.indicators = ['P/E', 'EPS (ttm)', 'Insider Own', 'Shs Outstand',
                           'Perf Week', 'Market Cap', 'Forward P/E', 'EPS next Y', 'Insider Trans',
                           'Shs Float', 'Perf Month', 'Income', 'PEG', 'EPS next Q', 'Inst Own',
                           'Short Float', 'Perf Quarter', 'Sales', 'P/S', 'EPS this Y', 'Inst Trans',
                           'Short Ratio', 'Perf Half Y', 'Book/sh', 'P/B', 'EPS next Y', 'ROA', 'Target Price',
                           'Perf Year', 'Cash/sh', 'P/C', 'EPS next 5Y', 'ROE', '52W Range', 'Perf YTD', 'Dividend',
                           'P/FCF', 'EPS past 5Y', 'ROI', '52W High', 'Beta', 'Dividend', 'Quick Ratio', 'Sales past 5Y',
                           'Gross Margin', '52W Low', 'ATR', 'Employees', 'Current Ratio', 'Sales Q/Q', 'Oper. Margin',
                           'RSI (14)', 'Volatility', 'Optionable', 'Debt/Eq', 'EPS Q/Q', 'Profit Margin', 'Rel Volume',
                           'Prev Close', 'Shortable', 'LT Debt/Eq', 'Earnings', 'Payout', 'Avg Volume', 'Price',
                           'Recom', 'SMA20', 'SMA50', 'SMA200', 'Volume', 'Change']
        self.key_names = {
            "P/E": "PE:1D",
            "EPS (ttm)": "EPS:Q_SUM:TTM",
            "Insider Own": "INSIDER_OWN",
            "Shs Outstand": "SHARES_OUT",
            "Perf Week": "PRICE:1D__CHANGE:1W",
            "Market Cap": "MARKET_CAP",
            "Forward P/E": "PE:1D_FW",
            "EPS next Y": "EPS:Y__ESTIMATE:1Y__AVERAGE:ALL",
            "Insider Trans": "INSIDER_TRS",
            "Shs Float": "SHARES_FLOAT:1Q",
            "Perf Month": "PRICE:1D__CHANGE:1Y",
            "Income": "NET_INCOME:1Y",
            "PEG": "PEG:1D",
            "EPS next Q": "EPS:Q__ESTIMATE:1Q__AVERAGE:ALL",
            "Inst Own": "INST_OWN:1Q",
            "Short Float": "SHORT_FLOAT:1Q",
            "Perf Quarter": "PRICE:1D__CHANGE:1Q",
            "Sales": "REVENUE:1Y",
            "P/S": "PS:1D",
            "EPS this Y": "EPS:1Y__ESTIMATE:1Y",
            "Inst Trans": "INST_TRS:1Q",
            "Short Ratio": "SHORT_RATIO:1Q",
            "Perf Half Y": "PRICE:1D__CHANGE:2Q",
            "Book/sh": "BOOK_PER_SHS:1Q",
            "P/B": "PB:1D",
            "EPS next Y": "EPS:1Y__ESTIMATE:2Y__AVERAGE:ALL",
            "ROA": "ROA:1Y",
            "Target Price": "PRICE:1D__ESTIMATE:1Y__AVERAGE:ALL",
            "Perf Year": "PRICE:1D__CHANGE:1Y",
            "Cash/sh": "CASH_PER_SHARE:1Q",
            "P/C": "PRICE_TO_CASH:1D",
            "EPS next 5Y": "EPS:1Y_ESTIMATE:5Y__CHANGE_A:5Y",
            "ROE": "ROE:1Y",
            "52W Range": "PRICE:1D__RANGE:1Y",
            "Perf YTD": "PRICE:1D__CHANGE:YTD",
            "Dividend": "DIVIDEND:1Y",
            "P/FCF": "PRICE_TO_FCF:1D",
            "EPS past 5Y": "EPS:1Y__CHANGE_A:5Y",
            "ROI": "ROI:1Y",
            "52W High": "PRICE:1D__HIGHEST_IN:1Y",
            "Beta": "BETA:1D",
            "Dividend": "DIVIDEND_YIELD:1D",
            "Quick Ratio": "QUICK_RATIO:1Q",
            "Sales past 5Y": "REVENUE:1Y__CHANGE_A:5Y",
            "Gross Margin": "GROSS_MARGIN:1Y",
            "52W Low": "PRICE:1D__LOWEST_IN:1Y",
            "ATR": "ATR",
            "Employees": "EMPLOYEES:1Q",
            "Current Ratio": "CURRENT_RATIO:1Q",
            "Sales Q/Q": "REVENUE:1Q__CHANGE:1Q",
            "Oper. Margin": "OPERATING_MARGIN:1Y",
            "RSI (14)": "RSI_14:1D",
            "Volatility": "VOLATILITY:1D",
            "Optionable": "OPTIONABLE:1Q",
            "Debt/Eq": "DEBT_TO_EQUITY:1Q",
            "EPS Q/Q": "EPS:1Q__CHANGE:1Q",
            "Profit Margin": "PROFIT_MARGIN:1Y"
            "Rel Volume": "REL_VOLUME:1D",
            "Prev Close": "PRICE:1D__PREV",
            "Shortable": "SHORTABLE",
            "LT Debt/Eq": "LT_DEBT_TO_EQUITY:1Q",
            "Earnings": "EARNINGS_DATE",
            "Payout": "PAYOUT:1Q",
            "Avg Volume": "VOLUME:1D__AVERAGE:ALL",
            "Price": "PRICE:1D",
            "Recom": "RECOMMENDATION_RANK:1D",
            "SMA20": "SMA_20:1D",
            "SMA50": "SMA_50:1D",
            "SMA200": "SMA_200:1D",
            "Volume": "VOLUME:1D",
            "Change": "PRICE:1D__CHANGE:1D"
        }

    def handle_data(self, data):
        if data in self.indicators:
            # Get custom key name istead of original.
            org_index = self.indicators.index(data)
            self.last_indicator = self.key_names[org_index]
            # We just set title of indicator, so value will be in next data
            # point. So we need to return after we set last_indicator
            return

        if self.last_indicator is not None:
            self.datamap[self.last_indicator] = data
            # After save of corresponding data point set last indicator
            # to None.
            self.last_indicator = None


class FinvizCompanyInfoParser(HTMLParser):

    def __init__(self):
        super().__init__()
        self.datamap = {}
        self.datalist = []
        self.fields = ['Ticker', 'Exchange',
                       'Name', 'Sector', 'Industry', 'Country']

    def handle_endtag(self, tag):
        if str(tag) == 'html':
            data = self.extract_data()
            for key, field in enumerate(self.fields):
                self.datamap[field] = data[key]

    def extract_data(self):
        data = []
        settings = False
        financial_highlights = False
        for row in self.datalist:
            row = row.strip()
            if row == 'Settings':
                settings = True
                continue
            if row == 'financial highlights':
                financial_highlights = True
            if settings == False:
                continue
            if financial_highlights == True:
                break
            data.append(row)
        return data

    def handle_data(self, data):
        data = data.strip()
        if len(data) != 0 and data != '|':
            self.datalist.append(data)






def get_page(symbol):
    """
    Download page from finviz.com with symbol provided.
    """
    try:
        formated_symbol = str(symbol).upper()
    except:
        return ValueError('Bad symbol was provided.')
    url = 'http://finviz.com/quote.ashx?t={0}'.format(formated_symbol)
    res = requests.get(url)
    if res.status_code == 200:
        return res.text


def parse_info(page):
    info = FinvizCompanyInfoParser()
    info.feed(page)
    return info.datamap


def parse_keystats(page):
    stats = FinvizKeyStatsParser()
    stats.feed(page)
    return stats.datamap


class KeyStatsController(object):

    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.store = FinvizStore()

    def update(self):
        # Result
        result = self.store.find(self.symbol)
        if result.count() == 0:
            print('Cannot find statistics.')
            result = self.download_new_data()
            return result
        else:
            return list(result)[:1]

    def download_new_data(self):
        page = get_page(self.symbol)
        keystats = parse_keystats(page)
        print(keystats)
        result = self.store.create_keystats(keystats, self.symbol)
        return result


