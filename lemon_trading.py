# Lemon SDK: https://github.com/lemon-markets/sdk-python
# pip install lemon


from keys_config import *
from lemon import api
from datetime import datetime
import pandas as pd
import random
import time

client = api.create(
    market_data_api_token=LEMON_MARKET_DATA_KEY,
    trading_api_token=LEMON_PAPER_TRADING_KEY,
    env='paper'  # or env='live'
)

stocks = pd.read_csv('Gettex_US.csv')

pd.DataFrame(client.market_data.venues.get().results).T

opening_days = client.market_data.venues.get('XMUN').results[0].opening_days

venue = client.market_data.venues.get('XMUN').results[0]
if not venue.is_open:
    opening_date: str = venue.opening_days[0].strftime('%d/%m/%Y')
    opening_time: str = venue.opening_hours.start.strftime('%H:%m')
    print(f'This exchange is closed at the moment. Please try again on {opening_date} at {opening_time}.')



# get instruments
ISINS=["US88160R1014", "US0231351067"]
data_isin = pd.DataFrame(client.market_data.instruments.get(ISINS).results)
pd.DataFrame(data_isin.venues[1])
pd.DataFrame.from_dict(client.market_data.instruments.get(ISINS).results)

response = client.market_data.instruments.get(ISINS)

etf_on_gettex =pd.DataFrame([])
page = 1
num_of_pages = client.market_data.instruments.get(search='***',type=['etf'],tradable=True,limit = 250).pages
while page <= num_of_pages:
    temp = pd.DataFrame(client.market_data.instruments.get(search='***',type=['etf'],tradable=True,limit = 250, page=page).results)
    etf_on_gettex = etf_on_gettex.append(temp)
    print('Done {} out of {}'.format(page,num_of_pages))
    page += 1
etf_on_gettex.reset_index(drop=True, inplace=True) # after resorting we need to reset index
etf_on_gettex.to_excel('etf_gettex_lemon.xlsx')
    
data_found = pd.DataFrame(client.market_data.instruments.get(search='t*a',tradable=True).results)
data_found = pd.DataFrame(client.market_data.instruments.get(search='***',tradable=True).results)
data_found = pd.DataFrame(client.market_data.instruments.get(search='S&P',type=['etf'],tradable=True).results)
data_found = pd.DataFrame(client.market_data.instruments.get(type=['stock', 'etf'],mic=['XMUN']).results) # "stock", "bond", "warrant", "fund", "etf"

# get latest ohlc
isins = data_found['isin'].to_list()[:9] # it seems only 10 allowed in 1 call
stocks_isin = stocks['ISIN'].to_list()
Latest_OHLC = pd.DataFrame(
                client.market_data.ohlc.get(
                    isin=stocks_isin[:8],
                    period='m1', # d1
                    from_='latest', # datetime(2022, 8, 2)
                    epoch=True,
                    decimals=True)
                .results)


# get latest quotes
        latest_quotes = pd.DataFrame(client.market_data.quotes.get_latest(isin=stocks_isin[:8], epoch=False,sorting='asc').results)

        idx = 0
        batch = 10 # not more than 10 in 1 request
        latest_quotes = pd.DataFrame([])
        # split request into several parts as it is too large
        while idx <= len(stocks_isin) - 1:
            temp = pd.DataFrame(client.market_data.quotes.get_latest(isin=stocks_isin[idx:idx+batch], epoch=False,sorting='asc').results)
            latest_quotes = latest_quotes.append(temp)
            print('Done till {} out of '.format(idx+batch,len(stocks_isin)))
            idx += batch
        latest_quotes.reset_index(drop=True, inplace=True) # after resorting we need to reset index





# get latest trades
latest_trades = pd.DataFrame(
                    client.market_data.trades.get_latest(
                        isin=['US88160R1014', 'US0231351067'], # not more than 10 in 1 request
                        epoch=True,
                        sorting='asc',
                        decimals=True
                        ).results)









# create buy order

response = client.trading.orders.create(isin='US88160R1014',side='buy',quantity=1)
order_id = response.results.id

order_id = 'ord_qyLfdYYHHSfq0rp1DJzPWFfZHzmQ844Nmn'


# below CreateOrder() structure is in response.results
    CreatedOrder(
        id='ord_qyLfdYYHHSfq0rp1DJzPWFfZHzmQ844Nmn', 
        status='inactive', 
        created_at=datetime.datetime(2022, 8, 24, 23, 19, 56, 155000, tzinfo=datetime.timezone.utc), 
        regulatory_information=RegulatoryInformation(
                costs_entry=10000, 
                costs_entry_pct='0.11%', 
                costs_runon_year=10000, 
                yield_reduction_year_pct='0.11%', 
                yield_reduction_year_following=0, 
                yield_reduction_year_following_pct='0.00%', 
                yield_reduction_year_exit=10000, 
                yield_reduction_year_exit_pct='0.11%', 
                estimated_holding_duration_years='5', 
                estimated_yield_reduction_total=20000, 
                estimated_yield_reduction_total_pct='0.22%', 
                KIID='text', 
                legal_disclaimer='The expected investment amount is based on the current buy or sell price or the selected limit or stop-loss price or stop-buy price. The actual execution price may differ. \
                                The assumed holding period for this security for the calculation of the absolute return reduction due to cost loading is 5 years. \
                                lemon.markets receives a payment of between € 0.00 and € 2.00 from the executing institution via its liability umbrella after execution of this transaction. \
                                In addition, lemon.markets bears the costs of this transaction \
                                charged by the executing institution, which are related to this transaction \
                                as well as to stock sizes (total annual transaction volume of lemon.markets). \
                                The actual payments received or granted are disclosed subsequently. \
                                Further information on this can be found in the customer documents.'), 
        isin='US88160R1014',
        expires_at=datetime.datetime(2022, 8, 25, 21, 59, tzinfo=datetime.timezone.utc), 
        side='buy', 
        quantity=1, 
        stop_price=None, 
        limit_price=None, 
        venue='xmun', 
        estimated_price=8932000, estimated_price_total=8932000, 
        notes=None, 
        charge=0, chargeable_at=None, 
        key_creation_id='435391e3-e7c2-4d05-9b10-9d13335b87fd', 
        idempotency=None)


# activate buy order
response = client.trading.orders.activate(order_id=order_id)

# get buy order status
response = client.trading.orders.get_order(order_id=order_id).results.executed_at
# below Order() structure is in response.results
    Order(
        id='ord_qyLfdYYHHSfq0rp1DJzPWFfZHzmQ844Nmn', 
        isin='US88160R1014', 
        isin_title='TESLA INC.', 
        expires_at=datetime.datetime(2022, 8, 25, 21, 59, tzinfo=datetime.timezone.utc), 
        created_at=datetime.datetime(2022, 8, 24, 23, 19, 56, 155000, tzinfo=datetime.timezone.utc), 
        side='buy', 
        quantity=1, 
        stop_price=None, 
        ce=0, 
        executed_price_total=0, 
        activated_at=datetime.datetime(2022, 8, 24, 23, 33, 2, 184000, tzinfo=datetime.timezone.utc), 
        executed_at=None, rejected_at=None, cancelled_at=None, 
        notes=None, 
        charge=0, chargeable_at=None, 
        key_creation_id='435391e3-e7c2-4d05-9b10-9d13335b87fd', 
        key_activation_id='435391e3-e7c2-4d05-9b10-9d13335b87fd', 
        regulatory_information=RegulatoryInformation(
                costs_entry=10000, 
                costs_entry_pct='0.11%', 
                costs_running=0, 
                costs_running_pct='0.00%', 
                costs_product=0, 
                costs_product_pct='0.00%', 
                costs_exit=10000, 
                costs_exit_pct='0.11%', 
                yield_reduction_year=10000, 
                yield_reduction_year_pct='0.11%', 
                yield_reduction_year_following=0, 
                yield_reduction_year_following_pct='0.00%', 
                yield_reduction_year_exit=10000, 
                yield_reduction_year_exit_pct='0.11%', 
                estimated_holding_duration_years='5', 
                estimated_yield_reduction_total=20000, 
                estimated_yield_reduction_total_pct='0.22%', 
                KIID='text', 
                legal_disclaimer='The expected inve.... in the customer documents.'), idempotency=None)


# get orders
    orders = client.trading.orders.get().results
    open_sell_orders = {order.id for order in orders if order.status == "executed" and order.type == "market"}

    all_executed_orders = pd.DataFrame(client.trading.orders.get().results)
    cash_to_invest+all_executed_orders.executed_price_total.sum()

# cancel order
response = client.trading.orders.cancel(order_id=response.results.id)


# Balance / Performance

    account_status = client.trading.account.get().results
    balance = account_status.balance # (Your end-of-day balance from the day before) + (amount_sold_intraday) - (amount_bought_intraday) - (amount_open_withdrawals)
    amount_bought_intraday = account_status.amount_bought_intraday
    amount_sold_intraday = account_status.amount_sold_intraday
    cash_to_invest = account_status.cash_to_invest # (balance) - (amount_open_orders)

    cash_to_invest = client.trading.account.get().results.cash_to_invest # (balance) - (amount_open_orders)


    # get positions

    current_positions_df = pd.DataFrame(client.trading.positions.get().results)
    response = client.trading.positions.get(isin='US88160R1014')

    response = client.trading.positions.get_performance()




# Other
    # get documents
    response = client.trading.account.get_documents()
    response = client.trading.account.get_document(document_id='doc_xyz')
    response = client.trading.user.get()
    response = client.trading.positions.get_statements()
    # get bank statements
    response = client.trading.account.get_bank_statements(
                    type='eod_balance',
                    from_="beginning"
                    )
