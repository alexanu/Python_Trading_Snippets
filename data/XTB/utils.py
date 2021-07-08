from itertools import product

import settings
from api import data_getter
from writer import write_tsv


def file_maker():
    combis = product(settings.WATCHLIST_MAP, settings.INDICATORS)

    for i in combis:
        s = i[0].split(",")[0]
        ind = i[1].split(",")
        data = data_getter(symbol=s, indicator=ind[0], period=ind[1])
        write_tsv(data=data[settings.TIMEFRAME], symbol=s, indicator=ind[0], period=ind[1])


if __name__ == '__main__':
    file_maker()
