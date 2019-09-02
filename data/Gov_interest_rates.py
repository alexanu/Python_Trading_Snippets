from datetime import datetime
from enum import Enum, unique
from os.path import join

import pandas as pd
import quandl
from pandas_datareader.data import DataReader
# Source: https://github.com/thoriuchi0531/adagio
        
from adagio.utils.date import date_shift

ROOT_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))
DATA_DIRECTORY = os.path.join(ROOT_DIRECTORY, 'data')

quandl_token = "XXXXXX"
START_DATE = datetime(1677, 9, 23)


@unique
class CashFile(Enum):
    USD_NYFED_DF = "M13009USM156NNBR"
    USD_3M_TBILL = "DTB3"
    USD_3M_LIBOR = "USD3MTD156N"
    EUR_3M_IB_RATE = "IR3TIB01DEM156N"
    EUR_3M_EURIBOR = "BOE/IUDERB3"
    GER_BANKRATE = "FRED/M13015DEM156NNBR"
    JPY_3M_LIBOR = "JPY3MTD156N"
    GBP_3M_LIBOR = "GBP3MTD156N"
    GBP_3M_LIBOR_M = "LIOR3MUKM"
    GBP_POLICY_RATE = "BOERUKM"


def load_usd():
    """ Return cash rate for USD """
    nyfed_df = DataReader(CashFile.USD_NYFED_DF.value, "fred", START_DATE)
    tbill = DataReader(CashFile.USD_3M_TBILL.value, "fred", START_DATE)
    libor = DataReader(CashFile.USD_3M_LIBOR.value, "fred", START_DATE)

    data = (pd.concat((to_monthend(nyfed_df[:"1953"]).fillna(method="pad"),
                       tbill['1954':"1985"].fillna(method="pad"),
                       libor['1986':].fillna(method="pad")),
                      axis=1)
            .sum(axis=1).rename("cash_rate_usd"))
    return data

def load_eur():
    """ Return cash rate for EUR and DEM prior to the introduction of EUR """
    bank_rate = quandl.get(CashFile.GER_BANKRATE.value, api_key=quandl_token)

    ww2_data = pd.DataFrame([4.0, 3.5, 5.0], index=[datetime(1936, 6, 30),datetime(1940, 4, 9),datetime(1948, 6, 28)])
    ww2_month = pd.date_range('1936-06-01', '1948-06-01', freq='M')
    ww2_month = pd.DataFrame(index=ww2_month)
    ww2_data = pd.concat((ww2_data, ww2_month), axis=1).fillna(method="pad")

    parser = lambda d: date_shift(datetime.strptime(d, "%Y-%m"),"+BMonthEnd")
    filename = join(DATA_DIRECTORY, 'cash_rate', 'eur', 'BBK01.SU0112.csv')
    discount_rate = pd.read_csv(filename, index_col=0,
                                skiprows=[1, 2, 3, 4], usecols=[0, 1], engine="python", skipfooter=95,
                                parse_dates=True, date_parser=parser)
    
    ib_rate = DataReader(CashFile.EUR_3M_IB_RATE.value, "fred", START_DATE)
    libor = quandl.get(CashFile.EUR_3M_EURIBOR.value, api_key=quandl_token)

    data = (pd.concat((bank_rate[:"1936-06"].fillna(method="pad"),
                       ww2_data,
                       discount_rate[:"1959"].fillna(method="pad"),
                       to_monthend(ib_rate['1960':"1998"].fillna(method="pad")),
                       libor['1999':].fillna(method="pad")),
                      axis=1)
            .sum(axis=1).rename("cash_rate_eur"))
    return data


def load_jpy():
    """ Return cash rate for JPY """
    libor = DataReader(CashFile.JPY_3M_LIBOR.value, 'fred', START_DATE)

    parser = lambda d: date_shift(datetime.strptime(d, "%Y/%m"),"+BMonthEnd")
    filename = join(DATA_DIRECTORY, 'cash_rate', 'jpy', 'discount_rate.csv')
    discount_rate = pd.read_csv(filename, index_col=0, usecols=[0, 1],
                                parse_dates=True, date_parser=parser)
    data = (pd.concat((discount_rate["1882-10":"1985-12"].astype("float").fillna(method="pad"),
                       libor['1986':].fillna(method="pad")),
                      axis=1)
            .sum(axis=1).rename("cash_rate_jpy"))
    return data

def load_gbp():
    """ Return cash rate for GBP """
    libor = DataReader(CashFile.GBP_3M_LIBOR.value, 'fred', START_DATE)
    libor_m = DataReader(CashFile.GBP_3M_LIBOR_M.value, "fred", START_DATE)
    policy_rate = DataReader(CashFile.GBP_POLICY_RATE.value, "fred", START_DATE)

    data = (pd.concat((policy_rate[:"1969-12"].fillna(method="pad"),
                       libor_m['1970':"1985"].fillna(method="pad"),
                       libor.fillna(method="pad")),
                      axis=1)
            .sum(axis=1).rename("cash_rate_gbp"))
    return data


def get_cash_rate(currency):
    """ Return historical cash rate for a given currency name """
    func_map = {
        "USD": load_usd,
        "EUR": load_eur,
        "JPY": load_jpy,
        "GBP": load_gbp,
    }
    cash_rate = func_map[currency]()
    return cash_rate

