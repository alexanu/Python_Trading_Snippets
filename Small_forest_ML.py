# Source: https://tr8dr.github.io//MLHardProblem1/

import talib
import pandas as pd
import numpy as np
from datetime import datetime
from pandas_datareader import data as pdr
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier

def getOHLC (stock: str, Tstart = datetime(1999,1,1), Tend = datetime.now()):
    raw = pdr.get_data_yahoo([stock], start=Tstart, end=Tend)
    df = raw[["Open","High","Low","Close","Adj Close","Volume"]]
    return df

bars = getOHLC ("SPY")
close = bars["Adj Close"].values.flatten()

# create features
df = bars.copy()
df["rsi"] = talib.RSI(close, timeperiod=14)
df["roc1"] = talib.ROC(close, timeperiod=1)
df["roc5"] = talib.ROC(close, timeperiod=5)
df["roc10"] = talib.ROC(close, timeperiod=10)
df["roc20"] = talib.ROC(close, timeperiod=20)
df["oc"] = np.log(bars.Close / bars.Open)
df["hl"] = np.log(bars.High / bars.Low)

# create { 0, 1 } labels, where 1 means 5 day return >= 50bps
df["label"] = (df["roc5"].shift(-5) >= 0.50) * 1.0

# feature columns
features = ["rsi", "roc1", "roc5", "roc10", "roc20", "oc", "hl"]

# split data set
icut = int(df.shape[0] * 0.70)
training = df.iloc[:icut].dropna()
testing = df.iloc[icut:].dropna()

# train model on features & labels
clf = RandomForestClassifier(n_estimators=500, random_state=1, n_jobs=-1)
model = clf.fit (training[features], training.label)

# use model to predict labels for training period and testing period respectively
pred_train = model.predict(training[features])
pred_test = model.predict(testing[features])

#  maximize TP (true-positive, our profitable trades) and minimize FP (false positives, out losing trades)
# less concerned with TN (true negative) and FN (false negative = missed opportunities, but no loss)
print(confusion_matrix(training.label, pred_train))
print(confusion_matrix(testing.label, pred_test))

# note that this is an overestimate, since may double count overlapping +1 label 5 day periods
print("P&L estimate: %1.0f%%" % (pred_test * testing.rfwd).sum())

