
# Source: https://github.com/migelekarapet/HierarchicalRiskParity


import numpy as np
import pandas as pd
from datetime import date
from matplotlib import pyplot as plt
import scipy.cluster.hierarchy as hier
import cvxopt as opt
import ffn
import config
from cvxopt import blas, solvers
from alpha_vantage.timeseries import TimeSeries



#here variance per each of clusters will be computed
def ClVar(cov,cItems):
    cov_=cov.loc[cItems,cItems]
    w_=getIVP(cov_).reshape(-1,1)
    cl_v=np.dot(np.dot(w_.T,cov_),w_)[0,0]
    return cl_v

#the clustered items would be sorted by disntance below
def sortByDist(link):
    link = link.astype(int)
    sortIx = pd.Series([link[-1, 0], link[-1, 1]])
    numItems = link[-1, 3]
    while sortIx.max() >= numItems:
        sortIx.index = range(0, sortIx.shape[0] * 2, 2)
        df0 = sortIx[sortIx >= numItems]
        i = df0.index
        j = df0.values - numItems
        sortIx[i] = link[j, 0]
        df0 = pd.Series(link[j, 1], index=i + 1)
        sortIx = sortIx.append(df0)
        sortIx = sortIx.sort_index() 
        sortIx.index = range(sortIx.shape[0])
    return sortIx.tolist()

#here we present portfolio with inverse variance
def getIVP(cov, **kargs):
    ivp = 1. / np.diag(cov)
    ivp /= ivp.sum()
    return ivp

#Hierarchical Risk Parity allocation will be computed below
def BisectRecurs(cov, sortIx):
    hrp = pd.Series(1, index=sortIx)
	#the items are stored and initialized in one cluster
    cItems = [sortIx]
    while len(cItems) > 0:
        cItems = [i[j:k] for i in cItems for j, k in ((0, len(i) // 2), (len(i) // 2, len(i))) if len(i) > 1]  # bi-section
        for i in range(0, len(cItems), 2):  # parse in pairs
			#the first one
            cItems0 = cItems[i]
			#the second cluster
            cItems1 = cItems[i + 1]
            cVar0 = ClVar(cov, cItems0)
            cVar1 = ClVar(cov, cItems1)
            alpha = 1 - cVar0 / (cVar0 + cVar1)
			#the first weight
            hrp[cItems0] *= alpha
			#the second one 
            hrp[cItems1] *= 1 - alpha
    return hrp


#below a disntance matrix based on correlation is comuted
def correlDist(corr):
    # the matrix of distance (non-euclidian) is presented below
    dist = ((1 - corr) / 2.)**.5 
    return dist

#Hierarchical portfolio is constructed below
def getHRP(cov, corr):
    dist = correlDist(corr)
    link = hier.linkage(dist, 'single')

    sortIx = sortByDist(link)
    sortIx = corr.index[sortIx].tolist()
    hrp = BisectRecurs(cov, sortIx)
    return hrp.sort_index()
	
	
	
	
def MinVarPortf(cov):

    cov = cov.T.values
    n = len(cov)
    N = 100
    mus = [10 ** (5.0 * t / N - 1.0) for t in range(N)]

    # convestion to cvxopt is performed below
    S = opt.matrix(cov)
    pbar = opt.matrix(np.ones(cov.shape[0]))

    
    G = -opt.matrix(np.eye(n))
    h = opt.matrix(0.0, (n, 1))
    A = opt.matrix(1.0, (1, n))
    b = opt.matrix(1.0)
	#in above 4 lines the contraint marices were constructed

    
	
	#Below we calculate the weights for Markowitz's efficient frontier using elementrs of quadr. programm.
    portfolios = [solvers.qp(mu * S, -pbar, G, h, A, b)['x']
                  for mu in mus]
    #Below the risks and returns for the frontier are calculated
    returns = [blas.dot(pbar, x) for x in portfolios]
    risks = [np.sqrt(blas.dot(x, S * x)) for x in portfolios]
    
	# below we calculate the second degree polynomial for the frontier
    m1 = np.polyfit(returns, risks, 2)
    x1 = np.sqrt(m1[2] / m1[0])
	
    # Finally, the optimal portfolio is constructed
    wt = solvers.qp(opt.matrix(x1 * S), -pbar, G, h, A, b)['x']

    return list(wt)
	
	
	
	def get_all_portfolios(returns):
    
    cov, corr = returns.cov(), returns.corr()
    hrp = getHRP(cov, corr)
    ivp = getIVP(cov)
    ivp = pd.Series(ivp, index=cov.index)
    mvp = MinVarPortf(cov)
    mvp = pd.Series(mvp, index=cov.index)
    
    portfolios = pd.DataFrame([mvp, ivp, hrp], index=['MVP', 'IVP', 'HRP']).T
    
    return portfolios
	
	
	stocks = {
    "Apple": "AAPL",
    "Amazon": "AMZN",
    "Microsoft": "MSFT",
    "Facebook": "FB",
	"Johnson & Johnson": "JNJ",
    "HYUNDAI MTR CO/GDR": "HYMTF",
    "Bank of America": "BAC"
    "Alibaba": "BABA",
    "Bank of America": "BAC",
    "Morgan Stanley": "MS",
    "BP": "BP",
 
}
stocks = pd.DataFrame(list(stocks.items()), columns=['name', 'symbol'])
ts = TimeSeries(key=config.key, output_format='pandas')
stocks_close = pd.DataFrame()
for symbol in stocks.symbol.values:
    data, _ = ts.get_daily(symbol=symbol, outputsize='full')
    close = data['4. close']
    close.index = pd.to_datetime(close.index)
    stocks_close = stocks_close.append(close)
stocks_close = stocks_close.T
stocks_close = stocks_close.sort_index()
stocks_close = stocks_close.fillna(method='ffill')
stocks_close.columns = stocks.name.values
stocks_close = stocks_close["2013-01-01":"2017-01-01"]
returns = stocks_close.to_returns().dropna()

portfolios = get_all_portfolios(returns)