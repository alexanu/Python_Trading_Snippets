#============================
# STRESS TEST
#============================

# To-do: - user-selected dates

import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import datetime
import pandas_datareader as pdr
import df2gspread
import quandl
import seaborn as sns

from pandas_datareader import data, wb
from df2gspread import gspread2df as g2d

#%matplotlib inline

#pd.set_option('display.expand_frame_repr', False)
np.seterr(divide='ignore', invalid='ignore')


def stress_test(event = 'none',  alpha = 0.05, compare = 'SPY', *args, **kwargs):
    start = ''
    end = ''
    
    pr = pd.read_csv("/home/rem/Documents/FXCM Trading (Dropbox)/Stress Test Data.csv", index_col=0)
        
    log_pr = np.log(pr)
    log_rt = np.around(log_pr.diff()*100, 1)
    log_rt = log_rt.drop(log_rt.index[0]).fillna(0)

    # Events
    if event == 'us':
        log_rt = log_rt.ix['2011-07-01':'2011-12-30']
        pr = pr.ix['2011-07-01':'2011-12-30']
        start = datetime.date(2011,7,1)
        end = datetime.date(2011,12,30)
        event_name = 'US DOWNGRADE'
    elif event == 'ch':
        log_rt = log_rt.ix['2015-08-01':'2016-04-29'] 
        pr =pr.ix['2015-08-01':'2016-04-29']
        start = datetime.date(2015,8,1)
        end = datetime.date(2016,4,29)
        event_name = 'CHINA DEVALUATION'
    elif event == 'br':
        log_rt = log_rt.ix['2016-06-01':'2016-07-15'] 
        pr = pr.ix['2016-06-01':'2016-07-15']
        start = datetime.date(2016,6,1)
        end = datetime.date(2016,7,15)
        event_name = 'BREXIT'
    elif event == 'all':
        log_rt1= log_rt.ix['2011-07-01':'2012-03-30']
        log_rt2 = log_rt.ix['2015-08-01':'2016-04-29']
        log_rt3 = log_rt.ix['2016-06-01':'2016-07-15'] 
        log_rt = pd.concat([log_rt1, log_rt2, log_rt3])
        pr1 = pr.ix['2011-07-01':'2012-03-30']
        pr2 =pr.ix['2015-08-01':'2016-04-29']
        pr3 = pr.ix['2016-06-01':'2016-07-15']
        pr = pd.concat([pr1, pr2, pr3])
        start = datetime.date(2011,7,1)
        end = datetime.date(2016,7,15)
        event_name = 'ALL'
    elif event == 'none':
        print('No event selected\n Options: US Downgrade (us), China Devaluation (ch), Brexit (br), all (all)', '\n')
        return
        
    weights = g2d.download(gfile="1bmy2DLu5NV5IP-mo9rGWOyHOx7bEfoglVZmzzuHi5zc", wks_name="Weights", col_names=True, row_names=True, credentials=None, start_cell='A1')
    weights = weights.apply(pd.to_numeric, errors='ignore')
    

    R = log_rt
    W = weights.WEIGHTS
    # T-do: Rename Z to rt_NAV 
    S = R.apply(lambda x: np.asarray(x) * np.asarray(W), axis=1) 
    S['rt_hist'] = np.round(S.sum(axis=1), 1)    
    
    returns = S.rt_hist
    sorted_returns = np.sort(returns)
    index = int(alpha * len(sorted_returns))
    var_stress = np.round(abs(sorted_returns[index])/100*weights.EQUITY[0], 0)
    sum_var = sorted_returns[0]
    for i in range(1, index):
        sum_var += sorted_returns[i]
    cvar_stress = np.round(abs(sum_var/index)/100*weights.EQUITY[0], 0)
    
    # Plot
    compare
    pr['Portfolio'] = S.rt_hist.cumsum()
    compare2 = kwargs.get('compare2', None)
    pr[[compare, compare2, 'Portfolio']].plot(subplots=True, title=event_name, layout=(3, 1), figsize=(12, 8), sharex=True) ;
    
    print('\n', 'Stress Test from', start, 'to', end, ' ', '***',event_name,'***', '\n')
    print('VaR (Stress.):', var_stress, '   ', 'CVaR (Stress.):', cvar_stress, '\n')

    return #(var_stress, cvar_stress) 


#stress_test(event='ch', compare='SPY')    
#stress_test(event='br', compare='SPY')    
#stress_test(event='us', compare='SPY')

def st(event='none', alpha = 0.05, compare='SPY', *args, **kwargs):
    if event != 'all':
        stress_test(event=event, alpha = alpha, compare=compare, *args, **kwargs)
        return
    elif event == 'all':
        stress_test(event='us', alpha = alpha, compare=compare, *args, **kwargs)
        stress_test(event='ch', alpha = alpha, compare=compare, *args, **kwargs)
        stress_test(event='br', alpha = alpha, compare=compare, *args, **kwargs)
        return






#====================
# STRESS TEST (LEGACY)
#====================
'''
def stress_test(start, end, compare = 'SPY', alpha = 0.05):
    start = datetime.date(*start)
    end = datetime.date(*end)
    slicekey = 'Adj Close'
    
    # Events
    # To-do: move this to global data
    if datetime.date(2015,12,31) < start < datetime.date(2017,1,1):
        event = 'BREXIT'
    elif datetime.date(2014,12,31) < start < datetime.date(2016,1,1):
        event = 'CHINA DEVALUATION' 
    elif datetime.date(2010,12,31) < start < datetime.date(2012,1,1):
        event = 'US DOWNGRADE'     
    elif datetime.date(2006,12,31) < start < datetime.date(2009,1,1):
        event = '2007-2008 FINANCIAL CRISIS'    
    else:
        event = 'UNKNOWN EVENT'
            
    tickers_instr = ['SPY', 'FEZ', 'GLD', 'SLV', 'JJC', 'USO', 'UNG', 'FXE', 'FXB', 'FXA', 'FXY', 'FXF', 'FXC', 'UUP']
    tickers_instr_quandl = ['CHRIS/EUREX_FGBL1.4', 'GOOG/NYSE_BNZ.4']  
    RawDataInstr = pdr.data.get_data_yahoo(tickers_instr, start, end)
    RawDataNZD =  quandl.get(tickers_instr_quandl[1], authtoken="PxdjCr_3nxsX_2JJd2si", start_date=start, end_date=end)
    pr_instr = np.round(RawDataInstr.ix[slicekey], 2)
    pr_instr['NZD'] = RawDataNZD
    # Reordering
    pr_instr = pr_instr[['SPY', 'FEZ', 'GLD', 'SLV', 'JJC', 'USO', 'UNG', 'FXE', 'FXB', 'FXA', 'FXY', 'FXF', 'FXC', 'NZD', 'UUP']]

    rt_instr = np.round(pr_instr.pct_change().dropna()*100, 2)
    
    weights = pd.read_excel('C:\\Users\\Ricardo\\Downloads\\FXCM Trading 2016 (Offline)\\Trading FXCM - 2016.xlsm', sheetname='Weights', index_col=0)

    R = rt_instr
    W = weights.WEIGHTS
    # T-do: Rename Z to rt_NAV 
    S = R.apply(lambda x: np.asarray(x) * np.asarray(W), axis=1) 
    S['rt_hist'] = np.round(S.sum(axis=1), 1)    
    
    returns = S.rt_hist
    sorted_returns = np.sort(returns)
    index = int(alpha * len(sorted_returns))
    var_stress = np.round(abs(sorted_returns[index])/100*weights.EQUITY[0], 0)
    sum_var = sorted_returns[0]
    for i in range(1, index):
        sum_var += sorted_returns[i]
    cvar_stress = np.round(abs(sum_var/index)/100*weights.EQUITY[0], 0)
    
    # Plot
    compare
    pr_instr['Portfolio'] = S.rt_hist.cumsum()
    pr_instr[[compare, 'Portfolio']].plot(subplots=True, title=event, layout=(2, 1), figsize=(10, 5), sharex=True) ;
    
    print('\n', 'Stress Test from', start, 'to', end, ' ', '***',event,'***', '\n')
    print('VaR (Stress.):', var_stress, '   ', 'CVaR (Stress.):', cvar_stress, '\n')

    return #(var_stress, cvar_stress) 
    
#stress_test((2011,7,1), (2012,3,30), compare='SPY')
#stress_test((2015,8,1), (2016,4,29), compare='SPY')    
#stress_test((2016,6,1), (2016,7,15), compare='SPY')     
'''

