import os
import math
import numpy as np
import pandas as pd
from pylab import plt, mpl

plt.style.use('seaborn')
mpl.rcParams['savefig.dpi'] = 300
mpl.rcParams['font.family'] = 'serif'
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.float_format', '{:.4f}'.format)
np.set_printoptions(suppress=True, precision=4)
os.environ['PYTHONHASHSEED'] = '0'

url = 'http://hilpisch.com/aiif_eikon_eod_data.csv'
symbol = 'EUR='
data = pd.DataFrame(pd.read_csv(url, index_col=0,parse_dates=True).dropna()[symbol])


data.info()

lags = 5
def add_lags(data, symbol, lags, window=20):
    cols = []
    df = data.copy()
    df.dropna(inplace=True)
    df['r'] = np.log(df / df.shift(1))
    df['sma'] = df[symbol].rolling(window).mean()
    df['min'] = df[symbol].rolling(window).min()
    df['max'] = df[symbol].rolling(window).max()
    df['mom'] = df['r'].rolling(window).mean()
    df['vol'] = df['r'].rolling(window).std()
    df.dropna(inplace=True)
    df['d'] = np.where(df['r'] > 0, 1, 0)
    features = [symbol, 'r', 'd', 'sma', 'min', 'max', 'mom', 'vol']
    for f in features:
        for lag in range(1, lags + 1):
            col = f'{f}_lag_{lag}'
            df[col] = df[f].shift(lag)
            cols.append(col)
    df.dropna(inplace=True)
    return df, cols

data, cols = add_lags(data, symbol, lags, window=20)

import random
import tensorflow as tf
from keras.layers import Dense, Dropout
from keras.models import Sequential
from keras.regularizers import l1
from keras.optimizers import Adam
from sklearn.metrics import accuracy_score

def set_seeds(seed=100):
    random.seed(seed)
    np.random.seed(seed)
    tf.random.set_seed(seed)
set_seeds()

optimizer = Adam(learning_rate=0.0001)

def create_model(hl=2, hu=128, dropout=False, rate=0.3, 
                regularize=False, reg=l1(0.0005),
                optimizer=optimizer, input_dim=len(cols)):
    if not regularize:
        reg = None
    model = Sequential()
    model.add(Dense(hu, input_dim=input_dim,activity_regularizer=reg,activation='relu'))
    if dropout:
        model.add(Dropout(rate, seed=100))
    for _ in range(hl):
        model.add(Dense(hu, activation='relu',activity_regularizer=reg))
        if dropout:
            model.add(Dropout(rate, seed=100))
    model.add(Dense(1, activation='sigmoid'))
    model.compile(loss='binary_crossentropy',optimizer=optimizer,metrics=['accuracy'])
    return model

split = '2018-01-01'
train = data.loc[:split].copy() # Splits the data into training and test data
np.bincount(train['d']) # frequency of the labels classes
array([ 982, 1006])








mu, std = train.mean(), train.std() # Normalizes the training features data
set_seeds()
model = create_model(hl=2, hu=64) # Creates the DNN model
model.fit(train_[cols], train['d'], epochs=20, verbose=False, validation_split=0.2, shuffle=False) # Trains the DNN model on the training data
model.evaluate(train_[cols], train['d']) # Evaluates the performance of the model on the training data




train['p'] = np.where(model.predict(train_[cols]) > 0.5, 1, 0) # binary predictions
train['p'] = np.where(train['p'] == 1, 1, -1) # Translates the predictions into position values
train['p'].value_counts() # number of long and short positions
train['s'] = train['p'] * train['r'] # strategy performance values
train[['r', 's']].sum().apply(np.exp) # gross performances (in-sample)
train[['r', 's']].sum().apply(np.exp) - 1 # net performances (in-sample)
train[['r', 's']].cumsum().apply(np.exp).plot(figsize=(10, 6));

test = data.loc[split:].copy() # Generates the test data sub-set
test_ = (test - mu) / std # Normalizes the test data
model.evaluate(test_[cols], test['d']) # Evaluates the model performance on the test data
test['p'] = np.where(model.predict(test_[cols]) > 0.5, 1, -1)
test['p'].value_counts()
test['s'] = test['p'] * test['r'] # strategy returns
test[['r', 's']].sum().apply(np.exp) # gross performances
test[['r', 's']].sum().apply(np.exp) - 1 # net performances
test[['r', 's']].cumsum().apply(np.exp).plot(figsize=(10, 6));

'''
The DNN-based trading strategy leads to a larger number of trades as compared to
the SMA-based strategy. This makes the inclusion of transaction costs an even more
important aspect when judging the economic performance.
'''
sum(test['p'].diff() != 0)
spread = 0.00012 # realistic bid-ask spreads for EUR/USD on the level of 1.2 pips
pc = spread / data[symbol].mean() # avg value for the proportional transaction costs pc is calculated ... 
                                  # ... based on the average closing price for EUR/USD
print(f'{pc:.6f}')

test['s_'] = np.where(test['p'].diff() != 0,test['s'] - pc, test['s']) # Adjusts the strategy performance for the transaction costs
test['s_'].iloc[0] -= pc # Adjusts the strategy performance for the entry trade
test['s_'].iloc[-1] -= pc # Adjusts the strategy performance for the exit trade
test[['r', 's', 's_']].sum().apply(np.exp)
test[['r', 's', 's_']].cumsum().apply(np.exp).plot(figsize=(10, 6));




