# pip install git+https://github.com/yhilpisch/tpqoa.git

import tpqoa
api = tpqoa.tpqoa('../pyalgo.cfg')

api.get_instruments()[:15] 

help(api.get_history)

instrument = 'EUR_USD' 
start = '2020-08-10' 
end = '2020-08-12' 
granularity = 'M1' 
price = 'M'

data = api.get_history(instrument, start, end, granularity, price)
data.info()
data[['c', 'volume']].head()

import numpy as np 
data['returns'] = np.log(data['c'] / data['c'].shift(1)) 
cols = [] 
for momentum in [15, 30, 60, 120]: 
    col = 'position_{}'.format(momentum) 
    data[col] = np.sign(data['returns'].rolling(momentum).mean()) 
    cols.append(col)

from pylab import plt 
plt.style.use('seaborn') 
import matplotlib as mpl 
mpl.rcParams['savefig.dpi'] = 300 
mpl.rcParams['font.family'] = 'serif' 
 
strats = ['returns'] 
 
for col in cols: 
    strat = 'strategy_{}'.format(col.split('_')[1])   
    data[strat] = data[col].shift(1) * data['returns'] 
    strats.append(strat) 
 
data[strats].dropna().cumsum().apply(np.exp).plot(figsize=(10, 6));

# amplifying effect on the performance of the momentum strategies for a leverage ratio of 20:1
data[strats].dropna().cumsum().apply(lambda x: x*20).apply(np.exp).plot(figsize=(10, 6));


# stream data
api.stream_data(instrument, stop=10) 
    # stop parameter stops the streaming after a certain number of ticks retrieved

api.create_order(instrument, 1000) # Opens a long position via market order
api.create_order(instrument, -1500) # Goes short after closing the long position via market order
api.create_order(instrument, 500) # Closes the short position via market order


# Strategy example
import tpqoa 
import numpy as np 
import pandas as pd 
 
 
class MomentumTrader(tpqoa.tpqoa): 
    def __init__(self, conf_file, instrument, bar_length, momentum, units, *args, **kwargs): 
        super(MomentumTrader, self).__init__(conf_file) 
        self.position = 0 
        self.instrument = instrument 
        self.momentum = momentum 
        self.bar_length = bar_length 
        self.units = units 
        self.raw_data = pd.DataFrame() 
        self.min_length = self.momentum + 1 # initial minimum bar length for the start of the trading itself

    def on_success(self, time, bid, ask): # method is called whenever new tick data arrives 
        ''' Takes actions when new tick data arrives. ''' 
        print(self.ticks, end=' ') # number of ticks retrieved is printed
        self.raw_data = self.raw_data.append(pd.DataFrame({'bid': bid, 'ask': ask}, index=[pd.Timestamp(time)])) 
        self.data = self.raw_data.resample(self.bar_length, label='right').last().ffill().iloc[:-1] # tick data is then resampled
        self.data['mid'] = self.data.mean(axis=1) # mid prices are calculated
        self.data['returns'] = np.log(self.data['mid'] / self.data['mid'].shift(1)) 
        self.data['position'] = np.sign(self.data['returns'].rolling(self.momentum).mean()) 

        if len(self.data) > self.min_length: # When there is enough or new data, ...
                                             # ... the trading logic is applied ....
            self.min_length += 1 # ... and min length is increased by one every time
            if self.data['position'].iloc[-1] == 1: 
                if self.position == 0: 
                    self.create_order(self.instrument, self.units) 
                elif self.position == -1: 
                    self.create_order(self.instrument, self.units * 2) 
                self.position = 1 
            elif self.data['position'].iloc[-1] == -1: 
                if self.position == 0: 
                    self.create_order(self.instrument, -self.units)
                elif self.position == 1: 
                    self.create_order(self.instrument, -self.units * 2) 
                self.position = -1


if __name__ == '__main__': 
    strat = 2 
    if strat == 1: 
        mom = MomentumTrader('../pyalgo.cfg', 'DE30_EUR', '5s', 3, 1) 
        mom.stream_data(mom.instrument, stop=100) 
        mom.create_order(mom.instrument, units=-mom.position * mom.units) #Closes out the final position
    elif strat == 2: 
        mom = MomentumTrader('../pyalgo.cfg', instrument='EUR_USD', bar_length='5s', momentum=6, units=100000) 
        mom.stream_data(mom.instrument, stop=100) 
        mom.create_order(mom.instrument, units=-mom.position * mom.units) #Closes out the final position
    else: 
        print('Strategy not known.')



api.get_account_summary()
api.get_transactions(tid=int(oo['id']) - 2)
api.print_transactions(tid=int(oo['id']) - 18)