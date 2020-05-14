import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import datetime
import pandas_datareader as pdr
import df2gspread
import statsmodels.api as sm
import quandl
import seaborn as sns

from pandas_datareader import data, wb
from pandas import ExcelWriter
from df2gspread import gspread2df as g2d
from df2gspread import df2gspread as d2g
from statsmodels.formula.api import ols

#%matplotlib inline

#pd.set_option('display.expand_frame_repr', False)
np.seterr(divide='ignore', invalid='ignore')

start = datetime.date.today()-datetime.timedelta(300)
end = datetime.date.today()-datetime.timedelta(1)
print('\v', '\v')
print('\t', '\t', 'DAILY DATA FOR', datetime.datetime.now().replace(microsecond=0), '\n', '\n')


#====================
# DATA
#====================
def get_day_data(start = start, end = end):
    #slicekey = 'Close'
    
    tickers_instr = ['CHRIS/CME_ES1.4', 'CHRIS/EUREX_FESX1.4', 'CHRIS/CME_NQ1.4', 'CHRIS/EUREX_FGBL1.4', 'CHRIS/ICE_DX1.4', 'CHRIS/CME_EC1.4', 'CHRIS/CME_BP1.4', 'CHRIS/CME_AD1.4', 'CHRIS/CME_JY1.4', 'CHRIS/CME_SF1.4', 'CHRIS/CME_CD1.4', 'CHRIS/CME_NE1.4', 'CHRIS/CME_CL1.4', 'CHRIS/CME_QG1.4', 'CHRIS/CME_GC1.4', 'CHRIS/CME_SI1.4', 'CHRIS/CME_HG1.4', 'CHRIS/EUREX_FDAX1.4']
    tickers_factors = ['SPY', 'DBP', 'JJC', 'USO', 'UNG', 'UUP', 'FXY']
    tickers_vol = ["CHRIS/CME_ES1.4", "CBOE/VIX.4", "CBOE/VXV.4", "CBOE/OVX", "CBOE/GVZ", "CBOE/VXSLV.4", "CBOE/EVZ"]
    
    RawDataInstr = quandl.get(tickers_instr, authtoken="PxdjCr_3nxsX_2JJd2si", start_date=start, rows=187)
    RawDataFactors = pdr.data.get_data_google(tickers_factors, start, end)
    RawDataVol = quandl.get(tickers_vol, start_date=start, end_date=end, api_key='PxdjCr_3nxsX_2JJd2si').tail(187)
    RawDataVol = RawDataVol.fillna(method='ffill')

    pr_instr = RawDataInstr.ffill()
    # Reordering
    pr_instr.columns = ['SPX500', 'EUSTX50', 'NAS100', 'BUND', 'USD', 'EUR', 'GBP', 'AUD', 'JPY', 'CHF', 'CAD', 'NZD', 'OIL', 'NGAS', 'GOLD', 'SILVER', 'COPPER', 'GER30']
    pr_factors = np.round(RawDataFactors.loc['Close'], 2).tail(187)
    pr_factors = pr_factors[['SPY', 'DBP', 'JJC', 'USO', 'UNG', 'UUP', 'FXY']]
    return (RawDataVol, pr_instr, pr_factors)

RawDataVol, pr_instr, pr_factors = get_day_data()





def get_day_stats(pr_instr=pr_instr, pr_factors=pr_factors, RawDataVol=RawDataVol):
    #print('\n', '\n', '\n')
    print("INSTRUMENTS\n", pr_instr.tail(2), '\n', '\n')
    print("FACTORS\n", pr_factors.tail(2), '\n', '\n')
    print("VOLATILITY\n", RawDataVol.iloc[:, 1:6].tail(2), '\n', '\n')
    
    # Price plot
    pr_instr.tail(74).plot(subplots=True, title="INSTRUMENTS", layout=(7, 3), figsize=(15, 25), sharex=True) ;
    print('\v')
    pr_factors.tail(74).plot(subplots=True, title="FACTORS", layout=(3, 3), figsize=(15, 10), sharex=True) ;
    print('\v')
    
    
    #====================
    # RETURNS
    #====================
    log_pr_instr = np.log(pr_instr)
    log_pr_fact = np.log(pr_factors)

    log_rt_instr = log_pr_instr.diff()
    log_rt_fact = log_pr_fact.diff()

    rt_instr = np.around(log_rt_instr.dropna()*100, 2)
    rt_fact = np.around(log_rt_fact.dropna()*100, 2)
    
    print('\n', "RETURNS\n", np.around(rt_instr.iloc[[-1]], 1), '\n')
    print(rt_fact.iloc[[-1]], '\n', '\n')
    
    # ewma-returns
    ewma_rt_instr = np.around(pd.DataFrame.ewm(rt_instr, span=5).mean(), 1).iloc[-1]
    ewma_rt_fact = np.around(pd.DataFrame.ewm(rt_fact, span=5).mean(), 1).tail(71)
    
    
    #====================================================
    # CORRELATION AND COVARIANCE MATRIX OF INSTRUMENTS
    #====================================================
    ewma_corr_instr = np.around(pd.DataFrame.ewm(rt_instr, span=32).corr().iloc[-1], 1)
    ewma_cov_instr = np.around(pd.DataFrame.ewm(rt_instr/100, span=32).cov().iloc[-1], 5)
    print('\n', "CORRELATION MATRIX OF INSTRUMENTS\n", ewma_corr_instr, '\n')
    print("COVARIANCE MATRIX OF INSTRUMENTS\n", ewma_cov_instr, '\n', '\n')
        
    ewma_corr_fact = np.around(pd.DataFrame.ewm(rt_fact, span=32).corr().iloc[-1], 1)
    ewma_cov_fact = np.around(pd.DataFrame.ewm(rt_fact/100, span=32).cov().iloc[-1], 5)
    print('\n', "CORRELATION MATRIX OF FACTORS\n", ewma_corr_fact, '\n')
    print("COVARIANCE MATRIX OF FACTORS\n", ewma_cov_fact, '\n', '\n', '\n')
     
    print('\v')
    sns.heatmap(ewma_corr_instr)
    sns.heatmap(ewma_cov_instr)

    # 1d std deviation of instruments
    ewma_var_instr = np.around(pd.DataFrame.ewm(rt_instr, span=32).var().tail(1), 1)
    ewma_std_instr = ewma_var_instr**(1/2)
    print("INSTRUMENTS STD. DEVIATION\n", np.around(ewma_std_instr, 1), '\n')
    
    # ann. volatility of instruments
    ewma_vol_instr = np.around(pd.DataFrame.ewm(rt_instr, span=32).std()*252**0.5, 1).dropna()
    print('\n', "INSTRUMENTS ANNUALIZED VOLATILITY\n", '\n', ewma_vol_instr.tail(1), '\n')
    ewma_vol_instr.tail(74).plot(subplots=True, title="VOLATILITY", layout=(10, 3), figsize=(15, 30), sharex=True) ;

    return (rt_instr, rt_fact, ewma_rt_instr, ewma_rt_fact, ewma_corr_instr, ewma_cov_instr, ewma_corr_fact, ewma_cov_fact, ewma_std_instr, ewma_vol_instr)
 
rt_instr, rt_fact, ewma_rt_instr, ewma_rt_fact, ewma_corr_instr, ewma_cov_instr, ewma_corr_fact, ewma_cov_fact, ewma_std_instr, ewma_vol_instr = get_day_stats()


#====================================================
# Vol Risk Premium & Term Structure
#====================================================
def vol_model(RawDataVol=RawDataVol):
    RawDataVol['vix_term'] = RawDataVol['CBOE/VXV - CLOSE']/RawDataVol['CBOE/VIX - VIX Close']
    RawDataVol['spx_ret'] = RawDataVol['CHRIS/CME_ES1 - Last'].pct_change()*100
    RawDataVol['ewma_vol_spx'] = pd.DataFrame.ewm(RawDataVol.spx_ret, span=32).std()*252**0.5
    #RawDataVol = RawDataVol.dropna()
    vol = pd.concat([ewma_vol_instr, RawDataVol], axis=1).dropna()
    
    # Plots
    print('\v')
    #RawDataVol['vix_term'].plot(title= 'VIX TERM STRUCTURE') ;
    RawDataVol[['CBOE/OVX - USO VIX (OVX)', 'CBOE/GVZ - GVZ', 'CBOE/VXSLV - Close', 'CBOE/EVZ - EVZ']].plot(subplots=1, title='IMPLIED VOLATILITY', layout=(2,2), figsize=(10, 5), sharex=True) ;
    #RawDataVol[['CBOE/VIX - VIX Close', 'CBOE/VXV - CLOSE']].tail(32).plot() ;
    print('\v')
    RawDataVol[['CBOE/VIX - VIX Close', 'ewma_vol_spx']].plot(title='S&P500 VOL RISK PREMIUM') ;
    #vol[['USO', 'CBOE/OVX - USO VIX (OVX)']].tail(32).plot(title='OIL VOL RISK PREMIUM') ;
    vol[['OIL', 'CBOE/OVX - USO VIX (OVX)']].plot(title='OIL VOL RISK PREMIUM') ;
    vol[['GOLD', 'CBOE/GVZ - GVZ']].plot(title='GOLD VOL RISK PREMIUM') ;
    vol[['SILVER', 'CBOE/VXSLV - Close']].plot(title='SILVER VOL RISK PREMIUM') ;
    vol[['EUR', 'CBOE/EVZ - EVZ']].plot(title='EURO FX VOL RISK PREMIUM') ;
    print('\v')    
    vol['vix_term_ema'] = vol['vix_term'].ewm(10).mean()
    vol[['vix_term', 'vix_term_ema']].plot(title= 'VIX TERM STRUCTURE') ;
    if vol['vix_term_ema'].ix[-1] >= 1.25: print("*** VIX TERM STRUTURE IN HEIGH CONTAGO ***")
    if vol['vix_term_ema'].ix[-1] <= 1.10: print("*** INVERTED VIX TERM STRUCTURE ***")
    
    return vol

vol = vol_model()


def exp_data(loc = 'gdrive'):
    'excel', 'gdrive'
    if loc == 'excel':
        writer  = ExcelWriter("/home/rem/Documents/FXCM Trading (Dropbox)/PyData.xlsx")
        pr_instr.to_excel(writer, 'Sheet1')
        pr_factors.to_excel(writer, 'Sheet2')
        ewma_corr_instr.to_excel(writer, 'Sheet3')
        ewma_cov_instr.to_excel(writer, 'Sheet4')
        vol.to_excel(writer, 'Sheet5')
        ewma_cov_fact.to_excel(writer, 'Sheet6')
        writer.save()
    if loc == 'gdrive':
        d2g.upload(pr_instr, gfile='/Trading FXCM/PyData', wks_name='pr_instr')
        d2g.upload(pr_factors, gfile='/Trading FXCM/PyData', wks_name='pr_factors')
        d2g.upload(ewma_corr_instr, gfile='/Trading FXCM/PyData', wks_name='corr_instr')
        d2g.upload(ewma_cov_instr, gfile='/Trading FXCM/PyData', wks_name='cov_instr')
        d2g.upload(ewma_cov_fact, gfile='/Trading FXCM/PyData', wks_name='cov_fact')
    return

exp_data()


#==============================================================================
# Till here just once a day
# From here variable part
#==============================================================================


#====================
# WEIGHTS
#====================
def get_weights(loc = 'cloud'):
    'local', 'cloud'    
    if loc == 'local':    
        weights = pd.read_excel('/home/rem/Documents/FXCM Trading (Dropbox)/Weights.xlsx', sheetname='Weights', index_col=0)
    if loc == 'cloud':
        #weights = pd.read_excel("https://1drv.ms/x/s!ApHwtSabAP46itkDw2YNwQHNAzCM4A", sheetname='Weights', index_col=0)
        weights = g2d.download(gfile="1bmy2DLu5NV5IP-mo9rGWOyHOx7bEfoglVZmzzuHi5zc", wks_name="Weights", col_names=True, row_names=True, credentials=None, start_cell='A1')
    #print('\n', 'Weights\n', weights, '\n', '\n')
    weights = weights.apply(pd.to_numeric, errors='ignore')
    return weights

#weights = get_weights()


#====================
# PORTFOLIO RISK
#====================
# To-do: add mean-var optimization
def portf_risk():
    weights = get_weights()

    # VaR single instruments    
    VaR_single = 1.645 * ewma_std_instr/100 * abs(weights.NOTIONAL)
    VaR_single = np.round(VaR_single, 0)
    #print('\n', "VaR\n", VaR, '\n')

    # Portfolio expected ret. and vol.    
    port_expRet = round(np.asscalar(np.dot(ewma_rt_instr.transpose(), weights[[0]])), 1)
    port_std = np.dot(np.transpose(np.dot(ewma_cov_instr, weights[[0]])), weights[[0]])
    port_std = np.asscalar(np.round(100*port_std**0.5, 1))
    port_vol = round(port_std*252**0.5)
    #print('\n', "Portfolio Risk\n", '\n')
    #print("Port. Exp. Ret.=", "%.1f%%" % port_expRet, '   ', 'Port. Std. Dev.=', "%.1f%%" % port_std, '   ', 'Port. Vol.=', "%.0f%%" % port_vol, '\n')

    # Betas of Instruments
    var = pd.DataFrame.ewm(rt_instr/100, span=32).var().dropna()['SPX500']
    var = np.around(var, 5)
    cov = pd.DataFrame.ewm(rt_instr/100, span=32).cov().dropna()
    cov = cov.xs(key='SPX500', axis=1).transpose()
    cov = np.around(cov, 5)
    beta = np.around(cov.div(var, axis='index'), 1)
    #print('\n', 'Instruments Beta\n', beta.iloc[[-1]], '\n')
    #beta.plot(subplots=True, title="INSTRUMENTS BETAS", layout=(5, 3), figsize=(15, 10), sharex=False) ;

    return (VaR_single, port_expRet, port_std, port_vol, beta.iloc[[-1]], beta)  

#VaR, port_expRet, port_std, port_vol, beta_last, beta = portf_risk()


#====================
# FACTOR MODEL
#====================
# Simulated Historical Returns (sim_NAV)
# (This is essentialy a backtest)
def get_fact_data():
    weights = get_weights()
    R = rt_instr
    W = weights.WEIGHTS

    # Sumproduct
    S = R.apply(lambda x: np.asarray(x) * np.asarray(W), axis=1) 
    S['rt_sim'] = np.round(S.sum(axis=1), 1)
    sim_NAV = S['rt_sim']
    # Returns
    rt_fact_mod = rt_fact.join(sim_NAV)   
    ewma_rt_fact_mod = np.round(pd.DataFrame.ewm(rt_fact_mod, span=5).mean(), 1)
    ewma_rt_fact_mod.rename(columns={'rt_sim':'ewma_sim_NAV'}, inplace=True)
    # reordering
    ewma_rt_fact_mod = ewma_rt_fact_mod[['ewma_sim_NAV', 'SPY', 'DBP', 'JJC', 'USO', 'UNG', 'UUP', 'FXY']]    
    #scatter_matrix(factors, alpha=0.8, diagonal='kde') ;
    #print('\n', 'Factors\n', ewma_rt_fact_mod.tail(2), '\n')    
    return (rt_fact_mod, sim_NAV, ewma_rt_fact_mod.tail(71))

#rt_fact_mod, sim_NAV, ewma_rt_fact_mod = get_fact_data()


def fact_model():
    rt_fact_mod, sim_NAV, ewma_rt_fact_mod = get_fact_data()

    # Betas of Factors (exp. weighted)
    varf = np.round(pd.DataFrame.ewm(rt_fact/100, span=32).var().dropna(), 5)
    covf = pd.DataFrame.ewm(rt_fact_mod/100, span=32).cov().dropna()
    covf = covf.xs(key='rt_sim', axis=1).transpose()
    betaf=pd.DataFrame([covf.SPY/varf.SPY, covf.DBP/varf.DBP, covf.JJC/varf.JJC, covf.USO/varf.USO, covf.UNG/varf.UNG, covf.UUP/varf.UUP, covf.FXY/varf.FXY]).transpose()
    betaf = np.round(betaf, 1)
    #print('Factors Beta\n', betaf.iloc[[-1]])
    #betaf.plot(subplots=True, title="FACTORS BETAS", layout=(5, 3), figsize=(15, 10), sharex=False) ;

    # Slopes from OLS
    fact_mod = ols(formula="ewma_sim_NAV~SPY+DBP+JJC+USO+UNG+UUP+FXY", data=ewma_rt_fact_mod).fit()
    #print('\n', 'Slopes\n', np.round(fact_mod.params, 1), '\n')
    #print('MSE\n', np.round(fact_mod.mse_resid, 2))
    #print('\n', 'Factor Model\n', fact_mod.summary(), '\n', '\n')
    #fig = plt.figure(figsize=(12,8))
    #fig = sm.graphics.plot_partregress_grid(fact_mod, fig=fig)
    
    # Fact. Model expected ret. and vol.
    factMod_expRet = round(np.asscalar(np.dot(ewma_rt_fact.iloc[[-1]], betaf.iloc[[-1]].transpose())+fact_mod.params[0]), 1)
    factMod_std = np.dot(betaf.iloc[[-1]], (np.dot(ewma_cov_fact, betaf.iloc[[-1]].transpose())))
    factMod_std = np.asscalar(np.round(100*factMod_std**0.5, 1))
    #factMod_std = np.round(factMod_std+fact_mod.mse_resid, 1)
    factMod_vol = round(factMod_std*252**0.5)
    #print("Factor Model\n")
    #print("Exp. Ret.=", "%.1f%%" % factMod_expRet, '   ', 'Std. Dev.=', "%.1f%%" % factMod_std, '   ', 'Vol.=', "%.0f%%" % factMod_vol, '   ', 'Id. Risk=', "%.1f%%" % fact_mod.mse_resid, '\n', '\n')

    return (betaf, fact_mod, factMod_expRet, factMod_std, factMod_vol)

#betaf, fact_mod, factMod_expRet, factMod_std, factMod_vol = fact_model()


#====================
# VALUE AT RISK
#====================
def VaR(returns, alpha = 0.05):
    weights = get_weights()
    factMod_std = fact_model()[3]
    sim_NAV = get_fact_data()[1]
    returns = sim_NAV
    
    # VaR from factor model
    var_fact = np.round(1.645*factMod_std/100*weights.EQUITY[0], 0)    

    # Historical simulation var
    sorted_returns = np.sort(returns)
    # Calculate the index associated with alpha
    index = int(alpha * len(sorted_returns))
    # VaR should be positive
    var_hist = np.round(abs(sorted_returns[index])/100*weights.EQUITY[0], 0)

    # CVar Conditional VaR of the returns
    # Calculate the total VaR beyond alpha
    sum_var = sorted_returns[0]
    for i in range(1, index):
        sum_var += sorted_returns[i]
    # CVaR return the average VaR (should be positive)
    cvar_hist = np.round(abs(sum_var/index)/100*weights.EQUITY[0], 0)
    #print('\n', 'VaR (fact.):', var_fact,  '   ', 'VaR (hist.):', var_hist, '   ', 'CVaR (hist.):', cvar_hist)

    return (var_fact, var_hist, cvar_hist) 



#====================
# STRESS TEST
#====================
def stress_test(event = 'none', compare = 'SPY', alpha = 0.05):
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
    pr[[compare, 'Portfolio']].plot(subplots=True, title=event_name, layout=(2, 1), figsize=(10, 5), sharex=True) ;
    
    print('\n', 'Stress Test from', start, 'to', end, ' ', '***',event_name,'***', '\n')
    print('VaR (Stress.):', var_stress, '   ', 'CVaR (Stress.):', cvar_stress, '\n')

    return #(var_stress, cvar_stress) 



#====================
# RISK SUMMARY
#====================
def risk_data():
    #get_weights()
    weights = get_weights()
    
    #portf_risk()
    VaR_single, port_expRet, port_std, port_vol, beta_last, beta = portf_risk()
   
    #get_fact_data()
    rt_fact_mod, sim_NAV, ewma_rt_fact_mod = get_fact_data()

    #fact_model()
    betaf, fact_mod, factMod_expRet, factMod_std, factMod_vol = fact_model()

    #VaR(returns=sim_NAV)    
    var_fact, var_hist, cvar_hist = VaR(returns=sim_NAV)

    return (weights, VaR_single, port_expRet, port_std, port_vol, beta_last, beta, rt_fact_mod, sim_NAV, ewma_rt_fact_mod, betaf, fact_mod, factMod_expRet, factMod_std, factMod_vol, var_fact, var_hist, cvar_hist)

#weights, VaR, port_expRet, port_std, port_vol, beta_last, beta, rt_fact_mod, sim_NAV, ewma_rt_fact_mod, betaf, fact_mod, factMod_expRet, factMod_std, factMod_vol = risk_data()


def risk_summary(plots = False, stress = 'none', ols = False):
    risk_data()    
    weights, VaR_single, port_expRet, port_std, port_vol, beta_last, beta, rt_fact_mod, sim_NAV, ewma_rt_fact_mod, betaf, fact_mod, factMod_expRet, factMod_std, factMod_vol, var_fact, var_hist, cvar_hist = risk_data()
       
    print('\n', '\n', '     ', 'RISK SUMMARY FOR', datetime.datetime.now().replace(microsecond=0), '\n', '\n')
    print('WEIGHTS\n', weights, '\n', '\n')    
    print('SINGLE POSITIONS VALUE AT RISK\n', '\n', VaR_single, '\n', '\n')
    VaR_lim = VaR_single.loc[:, (VaR_single >= 100).any(axis=0)]
    print('*** ALERT: POSITIONS EXCEEDING VaR LIMITS ***\n', '\n', VaR_lim, '\n', '\n', '\n')
    print('PORTFOLIO RISK\n')
    print('Port. Exp. Ret.=', "%.1f%%" % port_expRet, '   ', 'Port. Std. Dev.=', "%.1f%%" % port_std, '   ', 'Port. Vol.=', "%.0f%%" % port_vol, '\n', '\n')
    print('FACTOR MODEL\n')
    print('Exp. Ret.=', "%.1f%%" % factMod_expRet, '   ', 'Std. Dev.=', "%.1f%%" % factMod_std, '   ', 'Vol.=', "%.0f%%" % factMod_vol, '   ', 'Id. Risk=', "%.1f%%" % fact_mod.mse_resid, '\n', '\n', '\n')
    if port_std >= 5:
        print('*** ALERT: DAILY VOLATILITY EXCEEDING LIMIT ***', '\n', '\n', '\n')
    print('INSTRUMENTS BETA\n', '\n', beta_last,  '\n', '\n')
    print('FACTORS BETA\n', '\n', betaf.iloc[[-1]],  '\n', '\n', '\n')
    print('PORTFOLIO VALUE AT RISK\n', '\n', 'VaR (factors):', var_fact, '   ', 'VaR (hist.):', var_hist, '   ', 'CVaR (hist.):', cvar_hist,  '\n', '\n')
    if (var_fact or var_hist) > 800:
        print('*** ALERT: PORTFOLIO VaR TOO HIGH ***', '\n', '\n')
        
    # Plots
    if plots == True:
        beta.tail(100).plot(subplots=True, title="INSTRUMENTS BETAS", layout=(7, 3), figsize=(15, 20), sharex=True) ;
        betaf.tail(100).plot(title="FACTORS BETAS", figsize=(15,10)) 
        #betaf.tail(100).plot(subplots=True, title="FACTORS BETAS", layout=(5, 3), figsize=(15, 10), sharex=True) ;
        fig = plt.figure(figsize=(12,10))
        fig = sm.graphics.plot_partregress_grid(fact_mod, fig=fig)
    
    #OLS Summary    
    if ols == True: 
            print('\n', 'Factors Regression\n', fact_mod.summary(), '\n', '\n', '\n')

    #Stress Test
    stress  
    if stress != 'all':
        stress_test(event=stress)
        return
    # if stress == 'us':
    #     stress_test(event='us', compare='SPY')   
    # elif stress == 'ch':
    #     stress_test(event='ch', compare='SPY')    
    # elif stress == 'br':
    #     stress_test(event='br', compare='SPY')    
    elif stress == 'all':
        stress_test(event='us')
        stress_test(event='ch')    
        stress_test(event='br')
        return
    elif stress == 'none':
        print('No Stress-Test selected\n Options: US Downgrade (us), China Devaluation (ch), Brexit (br), all (all)', '\n', '\n')
    
    return
