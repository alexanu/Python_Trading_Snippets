# -*- coding: utf-8 -*-
import pandas as pd
import os
os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
#import Netfonds_Ticker_List as NTL   
import multiprocessing
import sys
import time
import StringIO
import datetime as dt
import urllib2
import socket
import io
import httplib

    
def reporthook(a,b,c): 
    # ',' at the end of the line is important!
    print "% 3.1f%% of %d bytes\r" % (min(100, float(a * b) / c * 100), c),

def single_intraday_pull_write(TCKR,date,extract='all', folder='', directory=''):
    """
    pulls intraday data, for one day, from netfonds.com
    """
    
    if type(date)!= type(pd.datetime(1,1,1).date()):
        print "Error:date is not datetime.datetime type"
        return 1
    
    datestr = date.strftime('%Y%m%d')    
    #time0=time.time()
    
    urls = []
    results = []
    if(extract=='all' or extract=='position' or extract=='combined'):     
        urls.append('http://hopey.netfonds.no/posdump.php?date=' 
                +datestr+'&paper=%s&csv_format=csv')
    if (extract=='all' or extract=='trades' or extract=='combined'):
        urls.append('http://hopey.netfonds.no/tradedump.php?date=' 
                +datestr+'&paper=%s&csv_format=csv')             
                
    for url in urls:
        urlread=0
        while urlread==0:   
            start=time.time()
            try:     
                sys.stdout.flush()
                buff = urllib2.urlopen(url %TCKR) 
                csvstring = buff.read()
                if 'This stock does not exist' in csvstring:
                    print TCKR+': This stock no longer exists'
                    urlread=1
                    break
                if csvstring =='': #no position data, move on to next url
                    break 
                else:
                    df = pd.read_csv(io.BytesIO(csvstring))
                    head_pos='time,bid,bid_depth,bid_depth_total,offer,offer_depth,offer_depth_total'
                    head_trade='time,price,quantity,board,source,buyer,seller,initiator'
                    df_names=','.join(df.columns.values)
                    if not ((head_pos==df_names) or (head_trade==df_names)):
                        print "Error: problem pulling data from website on date="+ datestr
                        break
                    elif len(df)==0:
#                        print "Market Closed or No Data on date="+ datestr
                        break
                    
                    df['time'] = pd.to_datetime(df['time'], format='%Y%m%dT%H%M%S')    
                    df = df.drop_duplicates()
                    df.index = df['time']; del df['time']
                    results.append(df)
                    urlread=1
            except urllib2.URLError, e:
                print TCKR + ' OOPS: timout error 1' + ' time='+str(time.time()-start)
                print e
            except socket.timeout, e:
                print TCKR + ' OOPS: timeout error 2' + ' time='+str(time.time()-start)
                print e
            except socket.error, e:
                print TCKR + ' OOPS: timeout error 3' + 'time='+str(time.time()-start)
                print e
            except httplib.IncompleteRead, e:
                print TCKR + ' OOPS: timeout error 4' + 'time='+str(time.time()-start)
                print e

    if len(results)==0: #return error 
        return 0
    elif len(results)==1:
        df = results[0].sort_index()
    elif len(results)>1:
        df = pd.concat(results).sort_index()        
    loc=directory+'\\Combined\\'+folder
    df.to_csv(loc+TCKR+'.'+datestr+'.csv')
        
    return 'ok'



def multi_intraday_pull2(TCKR,start=None,end=None, maxiter=30,extract='all', folder='', directory=''):
    """
    pulls intraday data, for multiple days, from netfonds.com
    """

    
    if (start!=None):
        if (type(start)!=type(pd.datetime(1,1,1).date())):
            print "Error:start is not typedatetime.datetime"
            return 1      
    if (end!=None):
        if (type(end)!=type(pd.datetime(1,1,1).date())):
            print "Error:end is not typedatetime.datetime"
            return 1        
    if (start==None and end != None):
        print "Error: must enter 'start' if you have entered 'end'"
        return 1
    if (start==None and end==None):
        end = pd.datetime.now()
        start = pd.datetime.min
    if (start!=None and end==None):
        end = pd.datetime.now().date()
        print "set end="+end.strftime('%Y%m%d')
    
    
    date = end    
    retcode=0
    i=0

    #run the loops to pull data on each day 
    while (retcode !=1 and date>=start):
        i+=1
        temp = sin.single_intraday_pull_write(TCKR,date,extract, folder, directory)   
        
        #check that an error code was not returned
        if (type(temp) == int):
            retcode = temp
            print TCKR +": singl_intraday_pull failed for iter="+str(i)+" and date="+date.strftime('%Y%m%d')
            date = date - dt.timedelta(days=1) 
            i-=1
            continue
            
        print TCKR +": succesfully pulled iter=" +str(i) + " for date="+date.strftime('%Y-%m-%d')
    
        date = date - dt.timedelta(days=1)
        
    return str(i)
  
  
  
  



def setup_parallel(tickers, mktdata='combined', n_process=3, 
                    baseDir = 'D:\\Financial Data\\Netfonds\\DailyTickDataPull', supress='yes'):
    
    #some args for the write file
    directory = baseDir
    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
    datestr = date.strftime('%Y%m%d')
       
    #break up problem into parts (number of processes)
    length = len(tickers)
    index=[]
    df_list=[]
    for i in range(n_process):
        index.append(range(i,length, n_process)) 
        df = tickers.loc[index[i]] 
        df.index=range(len(df))
        df_list.append(df)
    
    queue = multiprocessing.Queue()
    start = time.time()
    
    #read in latest_dates
    if not(os.path.isfile(directory+'\\latest_dates\\latest_dates.csv')):
        print 'No latest_date.csv file found'
        print 'program terminated'
        return        
        
    latest_dates_df = pd.read_csv(directory+'\\latest_dates\\latest_dates.csv', index_col = 0, header=0)
    latest_dates_df['latest_date'] = pd.to_datetime(latest_dates_df['latest_date'])  
    print 'Read Latest_dates using pd.read_csv'    
    
    #start the writing latest_dates file process
    w = multiprocessing.Process(target=write_latest_dates, args=(queue,latest_dates_df, directory, date, datestr, length))    
    w.start()  
    
    #start the pull data processes
    jobs=[]
    for tickers in df_list:
        p = multiprocessing.Process(target=pull_tickdata_parallel, args=(queue, tickers,latest_dates_df, 'combined', length, start, directory, supress))
        jobs.append(p)
        p.start()
    
    for j in jobs:
        j.join()
        
    print 'Joined other threads'
    queue.put('DONE')  #end the while loop in process 'w'
    w.join() #wait for join to happen
    print 'Joined the write thread'
    
    
    
def write_latest_dates(queue,latest_dates_df, directory, date, datestr, length):
    print 'Entered write_latest_dates function'    
          
    log_file_output = open(directory+'\\logfiles\\logfile'+ datestr +'.txt','w')
    log_file_output2 = open(directory+'\\logfiles\\logfile.txt','a')
    i=0
    while True:
        ret = queue.get()         

        if (type(ret) == tuple):
            i = i+1
            msg, tempstr = ret
            if msg.keys()[0] in latest_dates_df.index:
                latest_dates_df.ix[msg.keys()[0]]=msg.values()[0]
            else:
                latest_dates_df.set_value(index=msg.keys()[0], col='latest_date',value=msg.values()[0])
                print 'Added %s to latest_date file' %msg.keys()[0]
                
            latest_dates_df.to_csv(directory+'\\latest_dates\\latest_dates.csv')         
            latest_dates_df.to_csv(directory+'\\latest_dates\\latest_dates%s.csv'%datestr) 
            ind = tempstr.index('Iter=')
            tempstr=tempstr.replace(tempstr[ind:(ind+10)], 'Iter=%5d of %5d'%(i,length) )            
            print tempstr
            sys.stdout.flush()
            
            log_file_output.write(tempstr + '\n')
            log_file_output2.write(tempstr + '\n')            
            del msg[msg.keys()[0]]
            
        elif (ret == 'DONE'): #'DONE' is passed to queue from the main function when the data pull processed join()
            break
    
        else:
            print 'Error: ret from queue not as ecpected'
            print ret
            break
    
    log_file_output.close()
    log_file_output2.close()  
    return 
    


def pull_tickdata_parallel(queue, tickers, latest_date, mktdata='combined',nTot=0,sTime=0, directory='', supress='yes'):
    """
    pulls intraday data, for multiple days, for specified tickers, from netfonds.com
    """ 
    mktdata=mktdata.lower() #convert to lower case
    
    #get todays date, but with time of day set to zero    
    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
    ndays = 18

    pName = multiprocessing.current_process().name    
    
    for i in tickers.index:
        name = tickers['ticker'][i]
        folder=tickers['folder'][i]
        #get start date
        if (name in latest_date.index):
            start_date = (latest_date.latest_date.ix[name] + pd.offsets.BDay(1))
        else:
            start_date = date - pd.offsets.BDay(ndays)
            
        if start_date>date:
            print pName+ ':Iteration='+str(i) +' : Already collected data for '+name
            sys.stdout.flush()            
            continue
        
        #pull intraday data from the web for the current stock or index
        #positions, trades, combined 
        if supress=='yes': #suppresses the print statements in multi_intraday_pull2()
            sys.stdout = StringIO.StringIO()
            
        data = mul.multi_intraday_pull2(name, pd.datetime.date(start_date), date.date(), 30,mktdata, folder, directory)
        print pName+ ": %-3s daily files written: "%data +name +': Iter=%5d'%i +' completed: Starts:ends='+ start_date.strftime('%Y-%m-%d')+':'+date.strftime('%Y-%m-%d')
        
        if supress=='yes':
            sys.stdout = sys.__stdout__          
          
        tempstr = '%-12s: %-10s: Iter=%5d'%(pName,name,i)+ ', %-3s'%data +'dates complete in %5.2f min'%((time.time()-sTime)/60)        
        to_pass = ({name:date}, tempstr)
        queue.put(to_pass)  
        sys.stdout.flush()
    
    return 
    
if __name__=='__main__':    
    exper = ''  #\\temp  
    directory = 'D:\\Financial Data\\Netfonds%s\\DailyTickDataPull'%exper  
    ls=setup_parallel(toget=['ETF'], mktdata='combined', n_process=6,baseDir = directory)
    print 'hey'
