import mysql.connector
import pandas as pd
from crawl_nordnet import crawl_nordnet
import datetime as dt

def add_symbols_to_db(row, cursor):
    today = dt.datetime.now().date()
    symbols_data = []
    db_columns = 'stock_id, market_id, name, created, updated'
    db_values = ("%s, " * 5)[:-2]
    query_string = 'INSERT INTO symbols ({columns}) VALUES ({values})'.format(columns=db_columns, values=db_values)
    symbol = [row['stock_id'], row['market_id'], row['company_name'], today, 0]
    symbols_data.append(symbol)
    cursor.executemany(query_string, symbols_data)

def update_daily_price_data_to_db(cnx, cursor, stock_id, market_id, comp_name):
    df_symbols = pd.read_sql(
        'SELECT * FROM symbols WHERE stock_id = {stock_id}'.format(stock_id=stock_id),
        con=cnx)

    updated = df_symbols.iloc[0]['updated']
    print(updated)
    daily_data = []
    start = dt.datetime(2015, 1, 1).date()
    today = dt.datetime.now().date()
    #No data available for the symbol
    if updated == None:
        df = crawl_nordnet(stock_id, market_id, start, today)
        for index, row in df.iterrows():
            data = [stock_id, market_id, comp_name, row['Date'], row['Open'], row['High'],
                    row['Low'], row['Close'], row['Volume']]
            daily_data.append(data)
    elif today > updated:
        print('Updating data')
        df = Crawl_Nordnet(stock_id, market_id, today - dt.timedelta(days=7), today)
        for index, row in df.iterrows():
            if row['Date'] > updated:
                data = [stock_id, market_id, comp_name, row['Date'], row['Open'], row['High'],
                        row['Low'], row['Close'], row['Volume']]
                daily_data.append(data)
        Update_Symbol(cnx, cursor, stock_id, today)
    else:
        print('up to date')
    # Creating the query strings
    db_columns = 'stock_id, market_id, name, date, open, high, low, close, volume'
    db_values = ("%s, " * 9)[:-2]
    query_string = 'INSERT INTO daily_price ({columns}) VALUES ({values})'.format(columns=db_columns, values=db_values)
    print('Company: {} Stock_id: {} Market_id: {}'.format(comp_name, stock_id, market_id))
    cursor.executemany(query_string, daily_data)
    cnx.commit()


def Get_Symbol_Daily_Price(cnx, stock_id):
    df_db = pd.read_sql('SELECT * FROM daily_price WHERE stock_id = {stock_id} ORDER BY date ASC'.format(stock_id=stock_id),
                        con=cnx)
    #print(df_db.head(5))
    return df_db

def Get_Symbols(cnx):
    df_db = pd.read_sql('SELECT * FROM symbols', con=cnx)
    #print(df_db.head(5))
    return df_db

def Update_Symbol(cnx, cursor, stock_id, today):
    query_string = """UPDATE symbols SET updated = '{date}' WHERE stock_id = {stock_id}""".format(date=today,
                                                                                                  stock_id=stock_id)
    cursor.execute(query_string)
    cnx.commit()


def Connect_To_DB(host='localhost', user='stocks_adm', passw='fisklepp',db_name='stocks'):
    cnx = mysql.connector.connect(host=host, user=user, database=db_name, password=passw)
    cursor = cnx.cursor()
    return cnx, cursor


def Clear_DB(cursor, table):
    cursor.execute("TRUNCATE TABLE {table}".format(table=table))


def main():
    cnx, cursor = Connect_To_DB()
    #Clear_DB(cursor, 'daily_price')
    #Clear_DB(cursor, 'symbols')
    # Nordnet stocks
    nordnet_data = pd.read_csv('nordnet_tickers_2018-08-09.csv', sep=',', index_col=0)
    nordnet_data.drop_duplicates(inplace=True)
    for index, row in nordnet_data.iterrows():
        if row['market_id'] == 11:
            #Add_Symbols_To_DB(row, cursor)
            Update_Daily_Price_Data_To_DB(cnx, cursor, row['stock_id'], row['market_id'], row['company_name'])
    #Add_Symbols_To_DB(nordnet_data, cursor)
    stock_data = Get_Symbol_Daily_Price(cnx, 992)
    #cnx.commit()
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()
    
