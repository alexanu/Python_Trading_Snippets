# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os 
import time
import itertools
import multiprocessing
import sys
import StringIO
import glob

import Netfonds_Ticker_List as NTL
import get_lists as getl

def HDF_create_date_node(file_):
    """creates a node for all available dates in an existing HDF5 file"""
    store = pd.HDFStore(file_)
    dates = np.unique(store.dataframe.index.date)
    dates.sort()
    dates2 = pd.DataFrame(pd.to_datetime(dates), index=dates, columns=['dates'])
    store.put('dates', dates2, format='fixed',expectedrows=len(dates2))
    store.close()
    return
    
def HDF_create_date_nodes_multi(directories):
    """runs HDF_create_date_node() on all HDF5 files in the directories"""

    #get the files in the directories
    start_dir = os.getcwd() 
    files=[]
    i,j=0,0
    start = time.time()
    for directory in directories:
        j+=1
        os.chdir(directory)
        files=glob.glob("*.combined.h5") 
        n=len(files)
        for fl in files:
            i+=1
            HDF_create_date_node(fl)
            t=(time.time()-start)/60
            print 'completed date node for i=%d of %d, dir=%d, %-22s %-6.2f min'%(i,n,j,fl,t)
    os.chdir(start_dir)
    return t
    

def tick_data_combine_dates_multi(TICKERs,base_dir, ticker_folder, start, supress='yes'):
    """
    combines files across dates into single file for tickers in TICKERs
    TICKERs must be a list of tickers
    ticker_folder is a dictionary which specifies the folder each ticker can be found in
    start is the start time of the function 
    """
    i=0
    pName = multiprocessing.current_process().name
    for ticker in TICKERs:
        i+=1
        directory = ticker_folder[ticker]
        
        if supress=='yes': #supress print output of the single ticker function
            sys.stdout =  StringIO.StringIO()
    
        val = tick_data_combine_dates_single(ticker, 0, directory) # listdir=0 mean get_list will obtain listdir
        
        if supress=='yes': #restore print output
            sys.stdout=sys.__stdout__
            
        if val==1:
            print pName + ':%8s, %5s of %5s, no files to combine %-7.2f mins' %(ticker,i,len(TICKERs),((time.time()-start)/60))
        else:
            print pName + ':%8s, %5s of %5s, %s dates combined in %-7.2f mins' %(ticker,i,len(TICKERs),val,((time.time()-start)/60))
        sys.stdout.flush()
        
    return
    


def tick_data_combine_dates_single(ticker, listdir, directory=None):
    """
    Input: single ticker in format 'TICKER.X', where X is netfonds exchange letter (N:NYSE,O:NASDAQ,A:AMEX)
    Combines all tickdata files for the ticker in the directory, default = current.
    """
       
    start_dir = os.getcwd() #save start dir so we can revert back at the end of program
    if directory==None:
        directory = start_dir
    
    os.chdir(directory)

    #get list of files for ticker = ticker
    files = getl.get_csv_file_list(ticker, listdir, directory) 
    if files=='no tickers':
        return 'no tickers'

    """
    run case for no H5 file.
    if file doesn't exist, create it with the 1st csv data file, then close.    
    """
    if not(os.path.isfile(directory+'\\'+ticker+'.combined.h5')):
        store = pd.HDFStore(directory+'\\'+ticker+'.combined.h5')
        for fl in files: #find 1st file to create appendable h5 with, then break out of loop.
            if 'combined' in fl:
                continue
            temp = pd.read_csv(directory+'\\'+fl, header=0, index_col=0)
            if len(temp)==0:
                continue
            temp=temp[['bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total', 'price', 'quantity']]
            temp.index=pd.to_datetime(temp.index)
            temp = temp.sort_index()
            files.remove(fl)
            break
        store.append('dataframe', temp, format='table', complib='blosc', complevel=9,expectedrows=len(temp))
        #do the date node        
        temp_date = [temp.index[0].date()]
        temp_date =  pd.DataFrame(pd.to_datetime(temp_date), index=temp_date, columns=['dates'])
        store.put('dates', temp_date, format='fixed',expectedrows=len(temp_date))        
        store.close()
        
    
    
    """
    Now can run case where H5 file exists
    """
    store = pd.HDFStore(directory+'\\'+ticker+'.combined.h5')

    #get list of existing dates in the HDF5 data store
    if '/dates' in store.keys():
        olddates = list(store.dates.index) #return type is datetime.date
    elif len(store.dataframe)==0:
        olddates = []
    else: #return type is datetime.date
        olddates= list(pd.Series(store.dataframe.index).map(pd.Timestamp.date).unique()).sort()
        
    #get list of files to read in
    files2=[]
    newdates=[]
    for fl in files:
        if 'combined' in fl:
            continue
        date=pd.datetime.strptime(fl.replace('.csv','').replace(ticker+'.',''),'%Y%m%d').date()
        if date not in olddates:
            files2.append(fl)
            newdates.append(date)
        else:
            pass
        
    files=files2
            
                
    #read in the files to 'df'
    i=0
    df = pd.DataFrame()
    for fl in files:
        temp = pd.read_csv(directory+'\\'+fl, header=0, index_col=0)
        if len(temp)==0:
            continue
        temp=temp[['bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total', 'price', 'quantity']]
        df = df.append(temp)
        i+=1
    
    if len(df)>0:
        #convert index to timeindex
        if type(df.index) != pd.tseries.index.DatetimeIndex:
            df.index = pd.to_datetime(df.index)  
         
        #clean and append to the H5 data store.        
        df['index'] = df.index    
        df = df.drop_duplicates()  
        del df['index']
        df = df.sort_index()
        #dates= list(pd.Series(df.index).map(pd.Timestamp.date).unique())         
        #store.append('dates', dates, format='table',  complib='blosc', complevel=9, expectedrows=len(df))
        store.append('dataframe', df, format='table',  complib='blosc', complevel=9, expectedrows=len(df))
        
        assert len(newdates)>0, 'no new dates, but we have df>0'
        
        #create all_dates variable. unique values, sorted.
        all_dates = list(set(olddates + newdates))
        all_dates.sort()
        dates = pd.DataFrame(pd.to_datetime(all_dates), index=all_dates, columns=['dates'])
        #write to the store.    
        store.put('dates', dates, format='fixed',expectedrows=len(dates))
        
    else:
        assert len(newdates)==0, 'len(newdates)!=0, but len(df)==0'
        
    #close out the store
    store.close()
   
    os.chdir(start_dir)
    return str(i)
    
    
def combine_dates_multi_process_wrapper(TICKERs=None, indicies=None,  directories=None, n_process=3, supress='yes'):
    """
    all inputs must be of type list()
    
    TICKERs can represent an index (e.g. 'SPX', 'ETF', 'NYSE', 'NASDAQ', 'AMEX')
        * multiple incies must be passed as a list
    if TICKERs is not passed, acts on all files in the directory
    if directory is not passed, act within the current directory
    """    
    curdir = os.getcwd() # save starting directory so we can revert back at end of function    

    #Create dictionsry with {ticker:directory} pairs
    #This is needed to tell the single_ticker_combine file where to look for files
    dirs={}    
    if (TICKERs!=None):
        assert type(TICKERs)==list
        assert len(TICKERs)>0
        assert (indicies==None and directories==None)
        basedir='D:\\Financial Data\\Netfonds\\DailyTickDataPull'
        dirSPX = basedir + '\\Combined\\SPX'
        dirETF = basedir + '\\Combined\\ETF'
        dirNA = basedir + '\\Combined'
        
        #get list of tickers in the above directory
        ETFtickers = getl.get_list_tickers_in_dir(dirETF)[0]
        SPXtickers = getl.get_list_tickers_in_dir(dirSPX)[0]
        for ticker in TICKERs:
            if ticker in ETFtickers:
                dirs[ticker] = dirETF
            elif ticker in SPXtickers:
                dirs[ticker] = dirSPX
            else:
                dirs[ticker] = dirNA        

    elif (directories!=None): #extract the relevant tickers from the directories
        assert type(directories)==list
        assert len(directories)>0
        assert (TICKERs ==None and indicies==None)
        
        TICKERs = []
        lsdirs = []
        for directory in directories:
            #get list of tickers in the directory
            tckrs, listdir = getl.get_list_tickers_in_dir(directory)
            TICKERs.extend(tckrs)
            lsdirs.extend([directory]*len(tckrs))
        dirs = dict(itertools.izip(TICKERs,lsdirs))
    
    elif indicies!=None: #get list of tickers from netfonds website
        assert type(indicies)==list
        assert len(indicies)>0
        assert (TICKERs ==None and directories==None)
        TICKERs = NTL.get_netfonds_tickers(indicies)
        lsdirs = TICKERs.folder.values.tolist()
        TICKERs = TICKERs.ticker.values.tolist()
        dirs = dict(itertools.izip(TICKERs,lsdirs))
       
       
    #allocate tickers to different processes
    length = len(TICKERs)
    index=[]
    ls_list=[]
    for i in range(n_process):
        index.append(range(i,length, n_process)) 
        ls = [TICKERs[x] for x in index[i]]
#        df.index=range(len(df))
        ls_list.append(ls)
    

    start = time.time()            
    jobs=[]
    
    
    if n_process==1: #single process instance
        tick_data_combine_dates_multi(ls_list[0],curdir,dirs,start)
    else: #initiate seperate processes to combine dates
        for tickers in ls_list:
            p = multiprocessing.Process(target=tick_data_combine_dates_multi, args=(tickers,curdir,dirs,start, supress))
            jobs.append(p)
            p.start()
        for j in jobs:
            j.join()   
        print 'Joined other threads'       
    
    return
    
    
if __name__=='__main__':
    exper  =''# '\\temp' #for experimenting in the temp folder
    directory = 'D:\Financial Data\Netfonds'+exper + '\\DailyTickDataPull'
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
    directories = [directory + '\\Combined\\ETF']    
    combine_dates_multi_process_wrapper(TICKERs=None, indicies=None,  directories=directories, n_process=3, supress='yes')
        

