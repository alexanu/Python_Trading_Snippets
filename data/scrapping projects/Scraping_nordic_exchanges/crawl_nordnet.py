import requests
import pandas as pd
import numpy as np
import datetime as dt
import os

# https://www.nordnet.se/graph/indicator/sse/omxspi?from=1970-01-01&to=2018-08-04&fields=last
# https://www.nordnet.se/graph/instrument/11/992?from=1970-01-01&to=2018-08-08&fields=last,open,high,low,volume


def crawl_nordnet(stock_data, csv_path='csv/raw_csvs_'):
    start = dt.datetime(2015, 1, 1).date()
    end = dt.datetime.now().date()
    if not os.path.exists(csv_path):
        os.makedirs(csv_path)
    for i in range(len(stock_data)):
        # df.columns = ['company_name' ,'ticker' ,'market_id' ,'stock_id']
        stock_id = 0
        if 'stock_id' in stock_data.columns:
            stock_id = stock_data.loc[stock_data.index[i], 'stock_id']
        ticker = stock_data.loc[stock_data.index[i], 'ticker']
        csv_path_for_stock = '{path}/{stock}.csv'.format(path=csv_path, stock=ticker + '_' + str(stock_id))
        if not os.path.exists(csv_path_for_stock):
            try:
                market_id = stock_data.loc[stock_data.index[i], 'market_id']
                url = 'https://www.nordnet.se/graph/instrument/{market_id}/{stock_id}?from={start}&to={end}&fields=last,open,high,low,volume'.format(
                    market_id=market_id, stock_id=stock_id, start=start, end=end)
                r = requests.get(url)
                json_data = r.json()
                # creating dataframe
                df = pd.DataFrame.from_dict(json_data, orient='columns')
                df.columns = ['high', 'close', 'low', 'open', 'date', 'volume']
                df[['high', 'close', 'low', 'open']] = df[['high', 'close', 'low', 'open']].astype(np.float32).round(4)
                df['volume'] = df['volume'].astype(np.int64)
                df.fillna(method='ffill', inplace=True)
                df['date'] = pd.to_datetime(df['date'], unit='ms').dt.date
                if len(df) > 1:
                    df.to_csv(csv_path_for_stock)
            except:
                print('request error: {}'.format(stock_id))


def get_nordnet_stock_data():
    end = dt.datetime.now().date()
    #Bolagsnamn;Land;Lista;Sektor;Bransch;Ticker
    borsdata_nordic = pd.read_csv('borsdata_jan.csv', sep=';',encoding='latin-1')
    print(len(borsdata_nordic))
    borsdata_columns = ["company_name", "country", "list", "sector" ,  "industry", "ticker", "next_report",
                        "report_info", "1_year_dev", "yield", "pe", "ps", "pb", "eps_latest", "eps_1", "eps_3",
                        "rev_latest", "rev_1", "rev_3", "magic_rank", "market_id", "stock_id", "search"]
    nordnet_company_df = pd.DataFrame(columns=borsdata_columns)
    for x in range(len(borsdata_nordic)):
        try:
            column = borsdata_nordic.columns.values
            borsdata_nordic.columns = column
            company_name = borsdata_nordic.loc[borsdata_nordic.index[x], 'Bolagsnamn']
            country = borsdata_nordic.loc[borsdata_nordic.index[x], 'Info']
            list = borsdata_nordic.loc[borsdata_nordic.index[x], 'Info.1']
            sector = borsdata_nordic.loc[borsdata_nordic.index[x], 'Info.2']
            industry = borsdata_nordic.loc[borsdata_nordic.index[x], 'Info.3']
            ticker = borsdata_nordic.loc[borsdata_nordic.index[x], 'Info.4']
            next_report = borsdata_nordic.loc[borsdata_nordic.index[x], 'Info.5']
            last_report = borsdata_nordic.loc[borsdata_nordic.index[x], 'Info.6']
            one_year_dev = borsdata_nordic.loc[borsdata_nordic.index[x], 'Kursutveck.']
            yield_latest = borsdata_nordic.loc[borsdata_nordic.index[x], 'Direktav.']
            pe_latest =  borsdata_nordic.loc[borsdata_nordic.index[x], 'P/E']
            ps_latest = borsdata_nordic.loc[borsdata_nordic.index[x], 'P/S']
            pb_latest = borsdata_nordic.loc[borsdata_nordic.index[x], 'P/B']
            eps_latest = borsdata_nordic.loc[borsdata_nordic.index[x], 'Vinst/Aktie']
            eps_1 = borsdata_nordic.loc[borsdata_nordic.index[x], 'Vinst/Aktie.1']
            eps_3 = borsdata_nordic.loc[borsdata_nordic.index[x], 'Vinst/Aktie.2']
            rev_latest = borsdata_nordic.loc[borsdata_nordic.index[x], 'Omsätt./Aktie']
            rev_1 = borsdata_nordic.loc[borsdata_nordic.index[x], 'Omsätt./Aktie.1']
            rev_3 = borsdata_nordic.loc[borsdata_nordic.index[x], 'Omsätt./Aktie.2']
            magic_rank = borsdata_nordic.loc[borsdata_nordic.index[x], 'Magic']

            abbrev = 'se'
            if country == 'Sverige':
                abbrev = 'se'
            elif country == 'Finland':
                abbrev = 'fi'
            elif country == 'Norge':
                abbrev = 'no'
            elif country == 'Danmark':
                abbrev = 'dk'
            query =  ticker + ' ' + abbrev
            request = requests.post('https://www.nordnet.se/search/suggest.html', data={'q': query})
            data = request.json()
            stock = data['instruments'][0]
            stock_id = stock['identifier']
            if stock['instrument_group_type_name'] == 'Aktier' and stock_id not in nordnet_company_df.stock_id:
                search = stock['name']
                market_id = stock['market_id']

                nordnet_company_df = nordnet_company_df.append({'company_name' : company_name, 'country' : country,
                                                                'list' : list, 'sector' : sector, 'industry' : industry,
                                                                'ticker' : ticker, 'next_report' : next_report,
                                                                'last_report' : last_report, '1_year_dev' : one_year_dev,
                                                                'yield_latest' : yield_latest, 'pe_latest': pe_latest,
                                                                'ps_latest': ps_latest, 'pb_latest': pb_latest,
                                                                'eps_latest':eps_latest, 'eps_1':eps_1, 'eps_3': eps_3,
                                                                'rev_latest': rev_latest, 'rev_1':rev_1, 'rev_3': rev_3,
                                                                'magic_rank': magic_rank, 'market_id' : market_id,
                                                                'stock_id' : stock_id, 'search' : search}, ignore_index=True)

                print('{0:40} {1:20} {2:20} {3:20} {4:50} {5:6} {6:6} {7:6} {8:40}'.format(company_name, country, list, sector, industry, ticker, market_id, stock_id , search))
        except:
            print('Nothing found for ticker: {}'.format(ticker))

    #nordnet_company_df = pd.DataFrame(omx_tickers_nordnet, columns=['company_name', 'ticker', 'market_id', 'stock_id'])
    nordnet_company_df.to_csv('data_nordnet_{}.csv'.format(end))
    print('Tickers saved to data_nordnet_{}.csv'.format(end))
    '''
    tickerscsv = pd.read_csv('ndq_tickers.csv')
    tickers = tickerscsv['ticker'].values
    get_nordnet_stock_data(tickers)
    '''


def get_nordnet_stock_data_ndq():
    end = dt.datetime.now().date()
    # Bolagsnamn;Land;Lista;Sektor;Bransch;Ticker
    ndq_tickers = pd.read_csv('ndq_tickers_w_name.csv', sep=',')
    print(len(ndq_tickers))
    ndq_columns = ["company_name", "ticker", "market_id", "stock_id", "search"]
    ndq_company_df = pd.DataFrame(columns=ndq_columns)
    for x in range(len(ndq_tickers)):
        try:
            ticker = ndq_tickers.iloc[x].ticker
            company_name = ndq_tickers.iloc[x].company_name
            query = company_name.replace('-', ' ')
            request = requests.post('https://www.nordnet.se/search/suggest.html', data={'q': query})
            data = request.json()
            #print(data)
            stock = data['instruments'][0]
            stock_id = stock['identifier']
            if stock_id not in ndq_company_df.stock_id:
                search = stock['name']
                market_id = stock['market_id']
                ndq_company_df = ndq_company_df.append({'company_name': company_name, 'ticker': ticker,
                                                                'market_id': market_id, 'stock_id': stock_id,
                                                                'search': search},
                                                               ignore_index=True)

                print('{0:40} {1:6} {2:6} {3:6} {4:40}'.format(company_name, ticker, market_id, stock_id, search))
        except:
            print('Nothing found for ticker: {}'.format(company_name))

    ndq_company_df.to_csv('ndq_nordnet_{}_2.csv'.format(end))
    print('Tickers saved to ndq_nordnet_{}.csv'.format(end))
    '''
    tickerscsv = pd.read_csv('ndq_tickers.csv')
    tickers = tickerscsv['ticker'].values
    get_nordnet_stock_data(tickers)
    '''

#get_nordnet_stock_data()