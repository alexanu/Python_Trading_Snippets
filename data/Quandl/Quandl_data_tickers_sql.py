# source https://github.com/jdwittenauer/alpha/blob/master/fetch.py

import time
import urllib2
import Quandl
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import sql
from zipfile import ZipFile





def load_sql_data(engine, table=None, query=None, index=None, date_columns=None):
    """
    Retrieves data from a SQL database and loads the data into a data frame.
    """
    if query is not None:
        data = pd.read_sql(query, engine, index_col=index, parse_dates=date_columns)
    else:
        data = pd.read_sql('SELECT * FROM ' + table, engine, index_col=index, parse_dates=date_columns)

    print('SQL data loaded successfully.')

    return data


def save_sql_data(engine, data, table, exists='append', index=True, index_label=None):
    """
    Writes data from a data frame to a SQL database table.
    """
    data.to_sql(table, engine, schema=None, if_exists=exists,
                index=index, index_label=index_label, chunksize=10000)

    print('SQL data written successfully.')


def test_api_calls(api_key):
    """
    Test function with examples of calls using the Quandl API via python wrapper.
    """
    data = Quandl.get('WIKI/AAPL', authtoken=api_key)
    data_new = Quandl.get('WIKI/AAPL', rows=5, sort_order='desc', authtoken=api_key)
    data_range = Quandl.get('WIKI/AAPL', trim_start='2015-01-01', trim_end='2015-01-01', authtoken=api_key)
    data_multiple = Quandl.get(['WIKI/AAPL', 'WIKI/MSFT'], authtoken=api_key)


def get_dataset_codes(directory, dataset, api_key):
    """
    Retrieve the unique list of codes for the provided data set.
    """
    # retrieve the zip file
    codes_url = 'https://www.quandl.com/api/v3/databases/{0}/codes?api_key={1}'.format(dataset, api_key)
    response = urllib2.urlopen(codes_url)
    output = open(directory + '{0}_codes.zip'.format(dataset), 'wb')
    output.write(response.read())
    output.close()

    # unzip and load the csv into pandas
    z = ZipFile(directory + '{0}_codes.zip'.format(dataset))
    codes = pd.read_csv(z.open('{0}-datasets-codes.csv'.format(dataset)))

    # convert to a list
    code_list = codes.iloc[:, 0].tolist()

    # strip out excess white space characters
    code_list = map(lambda x: x.strip(), code_list)

    # pull out the descriptions as well
    description_list = codes.iloc[:, 1].tolist()
    description_list = map(lambda x: unicode(x, encoding='utf-8', errors='ignore'), description_list)

    print('Code retrieval successful.')
    return code_list, description_list


def create_dataset_table(engine):
    """
    Creates a table to track the names and status of data sets being fetched online.
    """
    dataset_list = ['WIKI', 'RAYMOND', 'FRED', 'FED', 'DOE']
    dataset_table = pd.DataFrame(dataset_list, columns=['Dataset'])
    dataset_table['Last Updated'] = datetime(1900, 1, 1)

    save_sql_data(engine, dataset_table, 'DATASETS', exists='replace', index=False)

    print('Dataset table creation complete.')


def create_stock_data_table(engine):
    """
    Creates and populates a table with general company information.
    """
    file_location = r'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/secwiki_tickers.csv'
    stock_data_table = pd.read_csv(file_location)
    save_sql_data(engine, stock_data_table, 'STOCK_INFO', exists='replace', index=False)

    print('Stock data table creation complete.')


def create_dataset_code_table(engine, directory, dataset, api_key):
    """
    Map the list of codes contained in a data set to a data frame to be used
    """
    update_dataset = sql.text(
        'UPDATE DATASETS '
        'SET [Last Updated] = :updated '
        'WHERE [Dataset] = :dataset')
    conn = engine.connect()

    # retrieve the most updated list from the remote server
    code_list, description_list = get_dataset_codes(directory, dataset, api_key)

    # strip off the data set name
    code_offset = len(dataset) + 1
    code_list_stripped = map(lambda x: x[code_offset:], code_list)

    # create a new frame with the dataset, codes, and placeholder columns for metadata
    code_table = pd.DataFrame(code_list_stripped, columns=['Code'])
    code_table['API Code'] = code_list
    code_table['Description'] = description_list
    code_table['Start Date'] = datetime(1900, 1, 1)
    code_table['End Date'] = datetime(1900, 1, 1)
    code_table['Last Updated'] = datetime(1900, 1, 1)
    code_table = code_table[['Code', 'API Code', 'Description', 'Start Date', 'End Date', 'Last Updated']]
    code_table = code_table.sort_values(by='Code')

    save_sql_data(engine, code_table, dataset + '_CODES', exists='replace', index=False)

    # update the dataset table to show that the code table was created
    conn.execute(update_dataset, updated=datetime.now(), dataset=dataset)

    print(dataset + ' code table creation complete.')


def load_historical_data(engine, dataset, api_key, mode='normal'):
    """
    Creates a new data table for the provided data set and loads historical data for each code into the table.
    """
    update_code = sql.text(
        'UPDATE ' + dataset + '_CODES '
        'SET [Start Date] = :start, [End Date] = :end, [Last Updated] = :updated '
        'WHERE Code = :code')
    conn = engine.connect()

    # retrieve the current code table
    code_table = load_sql_data(engine, dataset + '_CODES', date_columns=['Start Date', 'End Date', 'Last Updated'])

    # TESTING ONLY
    code_table = code_table.iloc[:10, :]

    # fetch the first code's data to create the data frame
    data = Quandl.get(code_table['API Code'].iloc[0], rows=1, authtoken=api_key)
    data = data.reset_index()
    data = data.rename(columns=lambda x: x[0] + x[1:].lower())
    data['Code'] = code_table['Code'].iloc[0]
    data = data.iloc[0:0]

    # iterate over each code and append the returned data
    counter = 0
    for index, row in code_table.iterrows():
        code_data = Quandl.get(row['API Code'], authtoken=api_key)
        code_data = code_data.reset_index()
        code_data = code_data.rename(columns=lambda x: x[0] + x[1:].lower())
        code_data['Code'] = row['Code']
        data = pd.concat([data, code_data])

        # update the code table
        min_date = code_data['Date'].min().to_datetime()
        max_date = code_data['Date'].max().to_datetime()
        current_date = datetime.now()
        conn.execute(update_code, start=min_date, end=max_date, updated=current_date, code=row['Code'])

        counter += 1
        if counter % 100 == 0:
            print('Sleeping for one minute to avoid hitting API call limits...')
            time.sleep(60)

    # move the code column to the beginning
    columns = data.columns.tolist()
    columns = [columns[-1]] + columns[:-1]
    data = data[columns]

    save_sql_data(engine, data, dataset, exists='replace', index=False)

    print(dataset + ' historical data loaded successfully.')


def update_dataset_codes(engine, directory, dataset, api_key):
    """
    Updates the list of codes for a data set and pulls in historical data for new codes.
    """
    select_code = sql.text('SELECT 1 FROM ' + dataset + '_CODES WHERE [Code] = :code')
    insert_code = sql.text(
        'INSERT INTO ' + dataset +
        '_CODES VALUES (:code, :api_code, :description, :start, :end, :updated)')
    update_dataset = sql.text(
        'UPDATE DATASETS '
        'SET [Last Updated] = :updated '
        'WHERE [Dataset] = :dataset')
    conn = engine.connect()

    # retrieve the most updated list from the remote server
    code_list, description_list = get_dataset_codes(directory, dataset, api_key)
    code_offset = len(dataset) + 1

    # iterate over each code and check its status in the database
    for index, api_code in enumerate(code_list):
        code = api_code[code_offset:]
        result = conn.execute(select_code, code=code)
        row = result.fetchone()

        # if there was no result then the code is new and must be inserted
        if row is None:
            description = description_list[index]
            init_date = datetime(1900, 1, 1)
            conn.execute(insert_code, code=code, api_code=api_code, description=description,
                         start=init_date, end=init_date, updated=init_date)

        result.close()

    # update the dataset table to reflect the fact that the code table was refreshed
    conn.execute(update_dataset, updated=datetime.now(), dataset=dataset)

    print(dataset + ' code table updated successfully.')


def update_dataset_data(engine, dataset, api_key):
    """
    Updates data for each code in the data set, retrieving new entries since the last update.
    """
    update_code = sql.text(
        'UPDATE ' + dataset + '_CODES '
        'SET [Start Date] = :start, [End Date] = :end, [Last Updated] = :updated '
        'WHERE Code = :code')
    conn = engine.connect()

    # retrieve the current code table
    code_table = load_sql_data(engine, dataset + '_CODES', date_columns=['Start Date', 'End Date', 'Last Updated'])

    # TESTING ONLY
    code_table = code_table.iloc[:10, :]

    # fetch the first code's data to create the data frame
    data = Quandl.get(code_table['API Code'].iloc[0], rows=1, authtoken=api_key)
    data = data.reset_index()
    data = data.rename(columns=lambda x: x[0] + x[1:].lower())
    data['Code'] = code_table['Code'].iloc[0]
    data = data.iloc[0:0]

    # iterate over each code and append the returned data
    counter = 0
    for index, row in code_table.iterrows():
        if row['Last Updated'] == datetime(1900, 1, 1):
            # this is a new code so we need to pull all historical data
            print('Loading historical data for new company {0}...'.format(row['API Code']))
            code_data = Quandl.get(row['API Code'], authtoken=api_key)
        else:
            # incremental update from the current end date for the code
            code_data = Quandl.get(row['API Code'], trim_start=str(row['End Date']), authtoken=api_key)

        # concat new data to the total set of new records
        code_data = code_data.reset_index()
        code_data = code_data.rename(columns=lambda x: x[0] + x[1:].lower())
        code_data['Code'] = row['Code']
        data = pd.concat([data, code_data])

        # update the code table
        min_date = code_data['Date'].min().to_datetime()
        max_date = code_data['Date'].max().to_datetime()
        current_date = datetime.now()
        conn.execute(update_code, start=min_date, end=max_date, updated=current_date, code=row['Code'])

        counter += 1
        if counter % 100 == 0:
            print('Sleeping for one minute to avoid hitting API call limits...')
            time.sleep(60)

    # move the code column to the beginning
    columns = data.columns.tolist()
    columns = [columns[-1]] + columns[:-1]
    data = data[columns]

    save_sql_data(engine, data, dataset, exists='append', index=False)

    print(dataset + ' code data updated successfully.')


def main():
    ex_init_database = False
    ex_update_database = True

    directory = 'C:\\Users\\jdwittenauer\\Documents\\Data\\Alpha\\'
    db_connection = 'sqlite:///C:\\Users\\jdwittenauer\\Documents\\Data\\Alpha\\alpha.db'
    api_key = 'SkQK_ZNrZn4cjfXxjJmb'

    engine = create_engine(db_connection)

    print('Beginning process...')

    if ex_init_database:
        create_dataset_table(engine)
        create_stock_data_table(engine)
        dataset_table = load_sql_data(engine, 'DATASETS', date_columns=['Last Updated'])
        for index, row in dataset_table.iterrows():
            create_dataset_code_table(engine, directory, row['Dataset'], api_key)
            load_historical_data(engine, row['Dataset'], api_key)

    if ex_update_database:
        create_stock_data_table(engine)
        dataset_table = load_sql_data(engine, 'DATASETS', date_columns=['Last Updated'])
        for index, row in dataset_table.iterrows():
            update_dataset_codes(engine, directory, row['Dataset'], api_key)
            update_dataset_data(engine, row['Dataset'], api_key)

    print('Process complete.')


if __name__ == "__main__":
    main()
