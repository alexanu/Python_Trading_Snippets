#
# wrapper class for  Oanda v20 API (RESTful & streaming)


import v20
import configparser
import pandas as pd
from v20.transaction import StopLossDetails


class tpqoa(object):

    def __init__(self, conf_file):
        ''' oanda.cfg:
        [oanda]
        account_id = ...
        access_token = ...
        account_type = practice (default) or live
      
        '''
        
        self.config = configparser.ConfigParser()
        self.config.read(conf_file)
        self.access_token = self.config['oanda']['access_token']
        self.account_id = self.config['oanda']['account_id']
        self.account_type = self.config['oanda']['account_type']

        if self.account_type == 'live':
            self.hostname = 'api-fxtrade.oanda.com'
            self.stream_hostname = 'stream-fxtrade.oanda.com'
        else:
            self.hostname = 'api-fxpractice.oanda.com'
            self.stream_hostname = 'stream-fxpractice.oanda.com'

        self.ctx = v20.Context(
            hostname=self.hostname,
            port=443,
            token=self.access_token,
        )
        self.ctx_stream = v20.Context(
            hostname=self.stream_hostname,
            port=443,
            token=self.access_token,
        )

        self.suffix = '.000000000Z'
        self.stop_stream = False

        
 # Setting-up the connection
# oanda = tpqoa('oanda.cfg')
        
        
        
    def get_instruments(self):
        ''' Retrieves and returns all instruments for the given account. '''
        resp = self.ctx.account.instruments(self.account_id)
        instruments = resp.get('instruments')
        instruments = [ins.dict() for ins in instruments]
        instruments = [(ins['displayName'], ins['name'])
                       for ins in instruments]
        return instruments
    
    # ins = oanda.get_instruments()
    # ins[:10]
    
    

    def transform_datetime(self, dati):
        ''' Transforms Python datetime object to string. '''
        if isinstance(dati, str):
            dati = pd.Timestamp(dati).to_pydatetime()
        return dati.isoformat('T') + self.suffix

    def retrieve_data(self, instrument, start, end, granularity, price):
        raw = self.ctx.instrument.candles(
            instrument=instrument,
            fromTime=start, toTime=end,
            granularity=granularity, price=price)
        raw = raw.get('candles')
        raw = [cs.dict() for cs in raw]
        if price == 'A':
            for cs in raw:
                cs.update(cs['ask'])
                del cs['ask']
        elif price == 'B':
            for cs in raw:
                cs.update(cs['bid'])
                del cs['bid']
        else:
            raise ValueError("price must be either 'B' or 'A'.")
        if len(raw) == 0:
            return pd.DataFrame()  # return empty DataFrame if no data
        data = pd.DataFrame(raw)
        data['time'] = pd.to_datetime(data['time'])
        data = data.set_index('time')
        data.index = pd.DatetimeIndex(data.index)
        for col in list('ohlc'):
            data[col] = data[col].astype(float)
        return data

    def get_history(self, instrument, start, end,
                    granularity, price):
        '''
        granularity: a string like 'S5', 'M1' or 'D'
        price: one of 'A' (ask) or 'B' (bid)
        '''
        if granularity.startswith('S') or granularity.startswith('M'):
            if granularity.startswith('S'):
                freq = '4h'
            else:
                freq = 'D'
            data = pd.DataFrame()
            dr = pd.date_range(start, end, freq=freq)
            for t in range(len(dr) - 1):
                start = self.transform_datetime(dr[t])
                end = self.transform_datetime(dr[t + 1])
                batch = self.retrieve_data(instrument, start, end,
                                           granularity, price)
                data = data.append(batch)
        else:
            start = self.transform_datetime(start)
            end = self.transform_datetime(end)
            data = self.retrieve_data(instrument, start, end,
                                      granularity, price)

        return data

    """data = oanda.get_history(instrument='EUR_USD',
                                start='2018-01-01', end='2018-08-09',
                                granularity='D',
                                price='A')"""
    
    
    def create_order(self, instrument, units, sl_distance=0.01):
        ''' Places order with Oanda.
        Parameters
        ==========
        instrument: string
            valid instrument name
        units: int
            number of units of instrument to be bought
            (positive int, eg 'units=50')
            or to be sold (negative int, eg 'units=-100')
        sl_distance: float
            stop loss distance price, mandatory eg in Germany
        '''
        sl_details = StopLossDetails(distance=sl_distance)
        request = self.ctx.order.market(
            self.account_id,
            instrument=instrument,
            units=units,
            stopLossOnFill=sl_details
        )
        order = request.get('orderFillTransaction')
        print('\n\n', order.dict(), '\n')
        
        
# going long 10,000 units
# sl_distance of 20 pips
#           oanda.create_order('EUR_USD', units=10000, sl_distance=0.002)  
# closing out the position
#           oanda.create_order('EUR_USD', units=-10000)
    
    
    

    def stream_data(self, instrument, stop=None, ret=False):
        ''' Starts a real-time data stream.
        Parameters
        ==========
        instrument: string
            valid instrument name
        '''
        self.stream_instrument = instrument
        self.ticks = 0
        response = self.ctx_stream.pricing.stream(
            self.account_id, snapshot=True,
            instruments=instrument)
        msgs = []
        for msg_type, msg in response.parts():
            msgs.append(msg)
            # print(msg_type, msg)
            if msg_type == 'pricing.ClientPrice':
                self.ticks += 1
                self.on_success(msg.time,
                                float(msg.bids[0].dict()['price']),
                                float(msg.asks[0].dict()['price']))
                if stop is not None:
                    if self.ticks >= stop:
                        if ret:
                            return msgs
                        break
            if self.stop_stream:
                if ret:
                    return msgs
                break

    def on_success(self, time, bid, ask): # Method called when new data is retrieved
        print(time, bid, ask)
        # print('BID: {:.5f} | ASK: {:.5f}'.format(bid, ask))
        
   # oanda.stream_data('EUR_USD', stop=3)     
   

    def get_account_summary(self, detailed=False):
        if detailed is True:
            response = self.ctx.account.get(self.account_id)
        else:
            response = self.ctx.account.summary(self.account_id)
        raw = response.get('account')
        return raw.dict()

    def get_transactions(self, tid=0):
        response = self.ctx.transaction.since(self.account_id, id=tid)
        transactions = response.get('transactions')
        transactions = [t.dict() for t in transactions]
        return transactions

    def print_transactions(self, tid=0):
         transactions = self.get_transactions(tid)
        for trans in transactions:
            try:
                templ = '%5s | %s | %9s | %12s | %8s'
                print(templ % (trans['id'],
                               trans['time'],
                               trans['instrument'],
                               trans['units'],
                               trans['pl']))
            except:
                pass
            
