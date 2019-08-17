import pandas as pd
import numpy as np
import argparse
import os
import datetime as dt
from tiingo import TiingoClient
import matplotlib.pyplot as plt

# ================= IMPORTANT - SET THESE TWO PARAMETERS
INTRADAY_DATA_DIR = os.path.expanduser('~/any place you want')
api_key = { 'api_key': 'your key goes here' }
# ============================================================

def to_list_of_tuples(x):
    ''' turn a tuple of lists into a list of tuples
    '''
    rtn = list()
    for j,row in enumerate(x):
        for i,val in enumerate(row):
            rtn.append((i,j,val))
    return rtn

def price_points(a):
    ''' Compute the control points for the day's high, close, low.
        a is the DF from a particular day.
        R's are sigmas above
        S's are sigmas below
    '''
    high = a['high'].max()
    low = a['low'].min()
    close = a['close'].iloc[-1]
    pivot = (high+close+low)/3.0
    S1 = 2*pivot - high
    R3 = high + 2.0*(pivot-low)
    R1 = 2.0*pivot-low
    R2 = pivot + R1 - S1
    S2 = pivot - R1 + S1
    S3 = low - 2*(high-pivot)
    
    return { 'S1': S1, 'S2': S2, 'S3': S3, 'R1': R1, 'R2': R2, 'R3': R3 }

def plot_MP_graphic(title, price, res, last_full_day):
    ''' Plot the graphics for all days provided.
        First create a psuedo_date with 2 hr intervals over the date range of interest. (max 14 TPOs)
        Then fill in the graphics based on the each days TPOs.
        pr contains the global time period price (mid-point)
        res is a list in trading date. Each element has:
            date: the trading date
            graphics: the tuple of elements (period_idx, price_idx, period_letter)
            POC_idx: the price_index representing the point of control
            POC_price: the price representing the point of control
            open: the day's opening price
    '''
    P = price_points(last_full_day)
    dprice = price[1]-price[0]
    ylim = ((min(P['S3'],price[0])-2*dprice),max(P['R3'],price[-1])+dprice)
    pr = price[:-1] + (price[1]-price[0])/2
    start_idx = 0
    for this_res in res:
        plt.plot((start_idx,start_idx+4), (this_res['open'],this_res['open']), color='orange', linestyle='-')
        for (period_idx, price_idx, letter) in this_res['graphics']:
            xy = (period_idx+start_idx,pr[price_idx])
            plt.scatter(xy[0],xy[1],0)
            plt.annotate(letter,xy=xy)
        plt.annotate('%02d-%02d' % (this_res['date'].month,this_res['date'].day),xy=(start_idx,ylim[0]+dprice))
        start_idx += 15
    plt.ylabel('Price ($)')
    frame1 = plt.gca()
    frame1.axes.get_xaxis().set_visible(False)
    
    xlim = plt.xlim()
    for label,price in P.items():
        plt.plot(xlim,(price,price), color='red', linestyle='--')
        plt.annotate('%s = $%0.2f' % (label,price), xy=(xlim[0],price))
    plt.ylim(ylim)
    plt.title(title)
    plt.show()
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='create the market profile for a particular symbol',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('symbol', help='symbol name or file', type=str)
    parser.add_argument('ndays', help='number of past days to download', nargs='?', type=int, default=7)
    parser.add_argument('-n', dest="nbins", type=int, default=50, help='number of bins')
    parser.add_argument("--offline", dest="offline", action="store_true", default=False, help="online so download recent data")
    args = parser.parse_args()
    args.symbol = args.symbol.upper()

    a = list()
    first_download = True
    today = dt.date.today()
    save_dir = os.path.join(INTRADAY_DATA_DIR,args.symbol)
    os.makedirs(save_dir,exist_ok=True)
    
    for i in range(-args.ndays,1,1):
        this_date = today + dt.timedelta(days=i)
        if this_date.weekday()>4: continue
        this_file = os.path.join(save_dir,'%s.pkl' % this_date)
        if os.path.isfile(this_file):
            a.append(pd.read_pickle(this_file))
        else:
            if not args.offline:
                if first_download:
                    client=TiingoClient(api_key)
                    first_download=False
                try:
                    b = client.get_dataframe(args.symbol,startDate=this_date,endDate=this_date,frequency='30min')
                    print('Downloading data from %s' % this_date)
                    if b.index[-1].hour==20 or this_date<today:      # save only if full day trading stops at 4 pm EST (naive)
                        b.to_pickle(this_file)
                    a.append(b)
                except Exception as err:
                    print('\tERROR %s' % err)
                    continue
            
    all = pd.concat(a)
    price = np.linspace(all['low'].min(),all['high'].max(),args.nbins)
    
    results = list()
    for df in a:
        per_idx = 0
        psuedo = tuple(list() for i in range(args.nbins))
        for ts,l,h in zip(df.index,df['low'].values,df['high'].values):
            for price_idx in np.where(np.logical_and(price>=l, price<h))[0]:
                psuedo[price_idx].append(chr(65+per_idx))
            per_idx += 1
       
        this_TPO = np.array(tuple(len(x) for x in psuedo))
        this_POC_idx = np.argmax(this_TPO)
        results.append({ 'graphics': to_list_of_tuples(psuedo),
                         'TPOs': np.array(tuple(len(x) for x in psuedo)),
                         'MP_str': tuple(''.join(x) for x in psuedo),
                         'date': df.index[0].date(),
                         'POC_idx': this_POC_idx,
                         'nTPO': np.sum(this_TPO),
                         'POC_price': price[this_POC_idx],
                         'open': df['open'].iloc[0]
                       })
        
    if a[-1].index[-1].hour == 20 or len(a)==1:     # determine last full day
        last_fullday_idx = -1
    else:
        last_fullday_idx = -2                           # take 2nd to last day otherwise
    plot_MP_graphic(args.symbol, price, results, a[last_fullday_idx])