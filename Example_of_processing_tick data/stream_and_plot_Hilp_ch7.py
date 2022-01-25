
import zmq # Python wrapper of ZeroMQ (lightweight, fast, and scalable socket programming library)
import math 
import time 
import random 
 
context = zmq.Context() 
socket = context.socket(zmq.PUB) # publisher-subscriber (PUB-SUB) pattern ...
                                 # ... where a single socket publishes data ...
                                 # ... and multiple sockets simultaneously retrieve the data
socket.bind('tcp://127.0.0.1:5555') # socket gets bound to the local IP address (127.0.0.1 on Windows) and the port number 5555
                        # another socket subscribing to the publishing socket is needed to complete the socket pair

# You need to execute the codes in different terminal instances, running different Python kernels.
# The execution within a single Jupyter Notebook will not work, however, ...
# ... the execution of the tick data server script in a terminal and the retrieval of data in a Jupyter Notebook will work


class InstrumentPrice(object): 
    def __init__(self): 
        self.symbol = 'SYMBOL' 
        self.t = time.time() 
        self.value = 100. # starting price
        self.sigma = 0.4 # constant volatility factor
        self.r = 0.01 # constant short rate

    def simulate_value(self): 
        ''' Generates a new, random stock price. 
            Geometric Brownian motion (without dividends) for which an exact Euler discretization is available
        ''' 
        t = time.time()           # When the .simulate_value() method is called, the current time is recorded
        dt = (t - self.t) / (252 * 8 * 60 * 60)         # time interval between the current time and the one stored in self.t in (trading) year fractions
        dt *= 500       # to have larger instrument price movements
        self.t = t      # attribute t is updated with the current time, which represents the reference point for the next call of the method
        self.value *= math.exp((self.r - 0.5 * self.sigma ** 2) * dt + self.sigma * math.sqrt(dt) * random.gauss(0, 1)) 
        return self.value 
 
ip = InstrumentPrice() 
 
while True: # infinite while loop
msg = '{} {:.2f}'.format(ip.symbol, ip.simulate_value()) 
print(msg)
socket.send_string(msg) # message is sent via the socket
                        # another socket subscribing to the publishing socket is needed to complete the socket pair
time.sleep(random.random()*2) # length of time interval between two publishing events it randomized


# -------------------------------------------------------------------------------------------------

import zmq

context = zmq.Context() 
socket = context.socket(zmq.SUB) # publisher-subscriber (PUB-SUB) pattern ...
                                 # ... where a single socket publishes data ...
                                 # ... and multiple sockets simultaneously retrieve the data
socket.connect('tcp://127.0.0.1:5555') # connect to the publishing socket
socket.setsockopt_string(zmq.SUBSCRIBE, 'SYMBOL') # subscribe to the "SYMBOL" channel (only available channel here)
                                                # In real-world applications, however, you might receive data ...
                                                # ... for a multitude of different symbols via a socket connection.
 
while True: # receiving data in an infinite loop
data = socket.recv_string() 
print(data)

# -------------------------------------------------------------------------------------------------
# Signal Generation in Real Time using the time series momentum of the last three five-second intervals

import zmq 
import datetime 
import numpy as np 
import pandas as pd 
 
context = zmq.Context() 
socket = context.socket(zmq.SUB) 
socket.connect('tcp://127.0.0.1:5555') 
socket.setsockopt_string(zmq.SUBSCRIBE, 'SYMBOL') 
 
df = pd.DataFrame() 
mom = 3 # number of time intervals for the momentum calculation
min_length = mom + 1 # initial minimum length for the signal generation to be triggered
 
while True: 
data = socket.recv_string() # retrieval of the tick data via the socket connection
t = datetime.datetime.now() 
sym, value = data.split() # string-based message is split into the symbol and the numerical value
df = df.append(pd.DataFrame({sym: float(value)}, index=[t])) 
dr = df.resample('5s', label='right').last() # tick data is resampled to a five-second interval, ...
                                             # ... taking the last available tick value as the relevant one
dr['returns'] = np.log(dr / dr.shift(1))  # log returns over the five-second intervals
if len(dr) > min_length: 
    min_length += 1 # increases min length of the resampled DataFrame object by one
    dr['momentum'] = np.sign(dr['returns'].rolling(mom).mean()) 
    print('\n' + '=' * 51) 
    print('NEW SIGNAL | {}'.format(datetime.datetime.now())) 
    print('=' * 51) 
    print(dr.iloc[:-1].tail()) # prints the final five rows of the resampled DataFrame
    if dr['momentum'].iloc[-2] == 1.0: # second but last value of the momentum column is used ...
                # ... since the last value is based at this stage on incomplete data for the relevant (not yet finished) time interval ...
                            # due to using the pandas .resample() method with the label='right' parametrization
        print('\nLong market position.') 
        # take some action (e.g., place buy order) 
    elif dr['momentum'].iloc[-2] == -1.0: 
        print('\nShort market position.') 
        # take some action (e.g., place sell order)

#------------------------------------------------------------------------------------------

import zmq 
import math 
import time 
import random 
 
context = zmq.Context() 
socket = context.socket(zmq.PUB) 
socket.bind('tcp://127.0.0.1:5555') 

# generates sample data for a streaming bar plot

while True: 
bars = [random.random() * 100 for _ in range(8)] 
msg = ' '.join([f'{bar:.3f}' for bar in bars]) 
print(msg) 
socket.send_string(msg) 
time.sleep(random.random() * 2)

# script above needs to be executed in a separate, local Python instance
# script below streams data as Bars
socket = context.socket(zmq.SUB)
socket.connect('tcp://127.0.0.1:5555')
socket.setsockopt_string(zmq.SUBSCRIBE, '')
for _ in range(5): 
    msg = socket.recv_string() 
    print(msg) 

fig = go.FigureWidget()
fig.add_bar()
fig

x = list('abcdefgh') 
fig.data[0].x = x 
for _ in range(25): 
    msg = socket.recv_string() 
    y = msg.split() 
    y = [float(n) for n in y] 
    fig.data[0].y = y

#-------------------------------------------------------------

# Visualization of streaming data in real time

'''
conda install plotly ipywidgets 
jupyter labextension install jupyterlab-plotly 
jupyter labextension install @jupyter-widgets/jupyterlab-manager 
jupyter labextension install plotlywidget
'''

import zmq 
from datetime import datetime 
import plotly.graph_objects as go

symbol = 'SYMBOL'

fig = go.FigureWidget() # instantiates a Plotly figure widget within the Jupyter Notebook
fig.add_scatter()
fig

context = zmq.Context() 
socket = context.socket(zmq.SUB) 
socket.connect('tcp://127.0.0.1:5555') 
socket.setsockopt_string(zmq.SUBSCRIBE, 'SYMBOL')

times = list()
prices = list()

for _ in range(50): 
    msg = socket.recv_string() 
    t = datetime.now() 
    times.append(t) 
    _, price = msg.split() 
    prices.append(float(price)) 
    fig.data[0].x = times 
    fig.data[0].y = prices

# ----------------------------------
# Ploting several streams
fig = go.FigureWidget() 
fig.add_scatter(name='SYMBOL') 
fig.add_scatter(name='SMA1', line=dict(width=1, dash='dot'), mode='lines+markers') 
fig.add_scatter(name='SMA2', line=dict(width=1, dash='dash'), mode='lines+markers') 
fig

import pandas as pd
df = pd.DataFrame()
for _ in range(75): 
    msg = socket.recv_string() 
    t = datetime.now() 
    sym, price = msg.split() 
    df = df.append(pd.DataFrame({sym: float(price)}, index=[t])) # Collects the tick data in a DataFrame object
        # resampling should be added to the implementation ...
        # ... since such trading algorithms are hardly ever based on tick data ...
        # ... but rather on bars of fixed length (five seconds, one minute, etc.).
    df['SMA1'] = df[sym].rolling(5).mean() 
    df['SMA2'] = df[sym].rolling(10).mean() 
    fig.data[0].x = df.index 
    fig.data[1].x = df.index 
    fig.data[2].x = df.index 
    fig.data[0].y = df[sym] 
    fig.data[1].y = df['SMA1'] 
    fig.data[2].y = df['SMA2']

# -----------------------------------
# Three Sub-Plots for Three Streams

from plotly.subplots import make_subplots
f = make_subplots(rows=3, cols=1, shared_xaxes=True) # Creates three sub-plots that share the x-axis
f.append_trace(go.Scatter(name='SYMBOL'), row=1, col=1) 
f.append_trace(go.Scatter(name='RETURN', line=dict(width=1, dash='dot'), mode='lines+markers', marker={'symbol': 'triangle-up'}), row=2, col=1)
f.append_trace(go.Scatter(name='MOMENTUM', line=dict(width=1, dash='dash'), mode='lines+markers', marker={'symbol': 'x'}), row=3, col=1) 
# f.update_layout(height=600) 
fig = go.FigureWidget(f)
fig

import numpy as np
df = pd.DataFrame()
for _ in range(75): 
    msg = socket.recv_string() 
    t = datetime.now() 
    sym, price = msg.split() 
    df = df.append(pd.DataFrame({sym: float(price)}, index=[t])) 
    df['RET'] = np.log(df[sym] / df[sym].shift(1)) 
    df['MOM'] = df['RET'].rolling(10).mean() 
    fig.data[0].x = df.index 
    fig.data[1].x = df.index 
    fig.data[2].x = df.index 
    fig.data[0].y = df[sym] 
    fig.data[1].y = df['RET'] 
    fig.data[2].y = df['MOM']