# -*- coding: utf-8 -*-
   
import pandas as pd
import urllib2
import io


def ExchangeTickers2CSV(url, ind,datestr,filepath):

    if ind=='AMEX':
        code = 'A'
    if ind=='NASDAQ':
        code = 'O'
    if ind=='NYSE':
        code = 'N'
    
    urlread=0
    #get ticker names from netfonds website
    while urlread==0:
        try:                
            buff = urllib2.urlopen(url %code) 
            csvstring = buff.read()
            if len(csvstring)==0:
                print 'No '+ind +' data for this date'
                print 'Using '+ind +' tickers from the .csv file on disk'
                urlread=1
                continue
            else:
                df = pd.read_csv(io.BytesIO(csvstring), sep=';', header=None)
                df.to_csv(filepath+ind+'.'+code+'.txt', sep=';', header=False, index=False)
                flname = ind+'.'+code+'.'+datestr+'.txt'
                df.to_csv(filepath+flname, sep=';', header=False, index=False)
                urlread=1
        except urllib2.URLError,e:
            print 'OOPS: URLError on '+ind +' ticker list pull'
            print e
        except socket.timeout, e:
            print TCKR + ' OOPS: socket timeout on '+ind +' ticker list pull'
            print e
        except socket.error, e:
            print TCKR + ' OOPS: socket error on '+ind +' ticker list pull'
            print e
        except httplib.IncompleteRead, e:
            print TCKR + ' OOPS: httplib Imcomplete error on '+ind +' ticker list pull'
            print e
    return 



def get_netfonds_tickers(toget=['SPX','ETF']):
    
    filepath = 'D:\\Financial Data\\Netfonds\\TickerList\\'
    temp = pd.DataFrame(columns=['ticker']) 
    folder = dict()
    date = pd.datetime.today() - pd.offsets.BDay(1)
    date=date.date()
    datestr = date.strftime('%Y%m%d')
    daystr=str(date.day)
    monthstr=str(date.month)
    yearstr=str(date.year)
    
    
    url='http://www.netfonds.no/quotes/exchange.php?'  
    url=url+'exchange=%s'
    url=url+'&at_day=' + daystr
    url=url+'&at_month=' +monthstr
    url=url+'&at_year=' +yearstr
    url=url+'&format=csv'
    #names = ['ticker','b','c','d','e','f','g','h']  
    NFunc.ExchangeTickers2CSV(url, 'AMEX',datestr,filepath)
    AMEX = pd.read_csv(filepath+'AMEX.A.txt', sep=';', header=None)
    AMEX.rename(columns={0:'ticker'},inplace=True) 
    NFunc.ExchangeTickers2CSV(url, 'NASDAQ',datestr,filepath) 
    NASDAQ = pd.read_csv(filepath+'NASDAQ.O.txt', sep=';',header=None)
    NASDAQ.rename(columns={0:'ticker'},inplace=True) 
    NFunc.ExchangeTickers2CSV(url, 'NYSE',datestr,filepath)
    NYSE = pd.read_csv(filepath+'NYSE.N.txt', sep=';',header=None)  
    NYSE.rename(columns={0:'ticker'},inplace=True) 
    
    if 'ETF' in toget:
#        todelete = []
        ETF = pd.read_csv(filepath+'ETF.txt',sep=',',header=0) #the file should alrady append the exchange letter
        #need to ID the exchange the SPX ticker is on
        tmp = ETF.ix[(~ETF.ticker.isin(AMEX.ticker+'.A') &~ETF.ticker.isin(NASDAQ.ticker+'.O') &~ETF.ticker.isin(NYSE.ticker+'.N'))].ticker 
        print "Error: Can't idendify ETF tickers exchange for the following:"
        print tmp.to_string()
        print "Error: Can't idendify ETF tickers exchange for the following:"
        ETF=ETF.drop(tmp.index)
        folder.update(dict((t, 'ETF') for t in ETF.ticker))
              
        ETF2 = pd.read_csv(filepath+'ETFvix.txt',sep=',',header=0) #need to ID the exchange the SPX ticker is on
        ETF2.loc[ETF2.ticker.isin(AMEX.ticker) &~ETF2.ticker.isin(NASDAQ.ticker) &~ETF2.ticker.isin(NYSE.ticker),'ticker']+='.A'
        ETF2.loc[ETF2.ticker.isin(NASDAQ.ticker) &~ETF2.ticker.isin(NYSE.ticker) &~ETF2.ticker.isin(AMEX.ticker),'ticker']+='.O'
        ETF2.loc[ETF2.ticker.isin(NYSE.ticker) &~ETF2.ticker.isin(AMEX.ticker) &~ETF2.ticker.isin(NASDAQ.ticker),'ticker']+='.N'
        tmp = ETF2.ix[(~ETF2.ticker.isin(AMEX.ticker+'.A') &~ETF2.ticker.isin(NASDAQ.ticker+'.O') &~ETF2.ticker.isin(NYSE.ticker+'.N'))].ticker 
        print "Error: Can't idendify ETF2 tickers exchange for the following:"
        print tmp.to_string()
        print "Error: Can't idendify ETF2 tickers exchange for the following:"
        ETF2=ETF2.drop(tmp.index)
        folder.update(dict((t, 'ETF') for t in ETF2.ticker))
        
        temp = pd.concat([temp,ETF])
        temp = pd.concat([temp,ETF2])
        toget.remove('ETF')        
        
    # SPX components are sourced from a website which extracts them from S&P
    if 'SPX' in toget:
#        todelete=[]
        url='https://raw.github.com/datasets/s-and-p-500-companies/master/data/constituents.csv'
        #(filename,headers)= ul(url)
        urlread=0
        while urlread==0:
            try:
                buff= urllib2.urlopen(url)
                cvsstring = buff.read()
                SPX = pd.read_csv(io.BytesIO(cvsstring),header=0)
                flname='SPX.'+datestr+'.txt'                
                SPX.to_csv(filepath+flname, sep=';', header=True, index=False)
                urlread=1
            except urllib2.URLError,e:
                print 'OOPS: URLError'
                print e
        #SPX = pd.read_csv(filename, header=0)
        SPX.rename(columns={'Symbol':'ticker'},inplace=True)   
        #need to ID the exchange the SPX ticker is on
        SPX.loc[SPX.ticker.isin(AMEX.ticker) &~SPX.ticker.isin(NASDAQ.ticker) &~SPX.ticker.isin(NYSE.ticker),'ticker']+='.A'
        SPX.loc[SPX.ticker.isin(NASDAQ.ticker) &~SPX.ticker.isin(NYSE.ticker) &~SPX.ticker.isin(AMEX.ticker),'ticker']+='.O'
        SPX.loc[SPX.ticker.isin(NYSE.ticker) &~SPX.ticker.isin(AMEX.ticker) &~SPX.ticker.isin(NASDAQ.ticker),'ticker']+='.N'
        tmp = SPX.ix[(~SPX.ticker.isin(AMEX.ticker+'.A') &~SPX.ticker.isin(NASDAQ.ticker+'.O') &~SPX.ticker.isin(NYSE.ticker+'.N'))].ticker 
        print "Error: Can't idendify SPX tickers exchange for the following:"
        print tmp.to_string()
        print "Error: Can't idendify SPX tickers exchange for the following:"
        SPX=SPX.drop(tmp.index)
        folder.update(dict((t, 'SPX') for t in SPX.ticker))
        toget.remove('SPX')
        temp = pd.concat([temp,SPX])
        

    if 'AMEX' in toget:                   
        AMEX['ticker'] = AMEX['ticker']+'.A' #append the .A for netfonds exchange ID 
        temp = pd.concat([temp,AMEX])
        toget.remove('AMEX')
                
    if 'NASDAQ' in toget:                  
        NASDAQ['ticker'] = NASDAQ['ticker']+'.O' #append the .O for netfonds exchange ID
        temp = pd.concat([temp,NASDAQ])
        toget.remove('NASDAQ')
        
    if 'NYSE' in toget:
        NYSE['ticker'] = NYSE['ticker']+'.N' #append the .N for netfonds exchange ID
        temp = pd.concat([temp,NYSE])
        toget.remove('NYSE')
        
     
    if (len(toget) != 0): #add remaing tickers to the list. This enables adding tickers not on the usual exchanges
        srs = pd.Series(toget)        
        data = pd.DataFrame(srs,columns=['ticker']) #append remaining tickers passed in 'toget'
        temp = temp.append(data)
    
    #temp = pd.concat([NYSE, AMEX, NASDAQ], ignore_index=True)
    temp = temp.drop_duplicates(subset ='ticker') #remove duplicates
    temp['folder']=''   
    for key_,val_ in folder.iteritems():
        temp.loc[temp.ticker==key_,'folder']=val_ +'\\'
            
    temp.index = range(len(temp.index))
    return temp[['ticker','folder']]   
    
if __name__=='__main__':
    tickers = get_netfonds_tickers(toget=['SPX','ETF','AMEX','NYSE','NASDAQ']) 
    print 'hey'
