# -*- coding: utf-8 -*-

import tarfile
import gzip
import bz2
import os
import pandas as pd
import get_lists as getl
import string
import cStringIO

def digits_only(s):
    all = string.maketrans('','')
    nodigs=all.translate(all, string.digits)
    return s.translate(all, nodigs)
    
    

def zip_data_to_hdf_single_ticker(ticker, directory, compresstype = 'bz2'):

    retval='& none added 2 H5'    
    tarfl = directory + '\\'+ticker+'.tar'
    tar = tarfile.open(tarfl,'r')
    archived_files = tar.getnames() 
    hdf = directory+'//'+ticker+'.combined.h5'
    if not (os.path.isfile(hdf)):
        s='%-8s: need to setup if hdf5 file does not exist'%ticker
        print s
        f = open('D:\\Financial Data\\Netfonds\\DailyTickDataPull\\errorlog.txt','a')
        f.write(s+'\n')
        f.close()
        return            

    store = pd.HDFStore(hdf) #open store
    olddates= list(pd.Series(store.dataframe.index).map(pd.Timestamp.date).unique())
    store.close()    
    
    #get list of dates from filenames
    zipdates=[]
    for fl in archived_files:
        fl = digits_only(fl) #extract the dates from the filename
        if compresstype=='bz2':
            fl = fl[:-1]
        try:
            fldate = pd.datetime.strptime(fl,'%Y%m%d').date()
            zipdates.append(fldate)
        except ValueError, e:
            print e
            s='%-8s: Cant convert filename to date' %ticker
            print s
            f = open('D:\\Financial Data\\Netfonds\\DailyTickDataPull\\errorlog.txt','a')
            f.write(s+'\n')
            f.write(e+'\n')
            f.close()
    
    #get archived dates not in hdf5 dates
    newdates = [x for x in zipdates if x not in olddates]
    
    #get filenames corresponding to new dates
    files=[]
    for d in newdates:
        flname=ticker+'.'+d.strftime('%Y%m%d')+'.csv.'+compresstype
        files.append(flname)

    #proceed to uncompress each file and append to a dataframe            
    df = pd.DataFrame()            
    for fl in files:
        tempfl = tar.extractfile(fl)
        compdata = tempfl.read()
        if compresstype=='bz2':
            data = bz2.decompress(compdata)
        elif compresstype=='gz':
            compdatab = cStringIO.StringIO(compdata)
            unctempfl = gzip.GzipFile('dummy-name',mode='rb',fileobj=compdatab)
            data = unctempfl.read()            
            unctempfl.close()
            
        tempdf = pd.read_csv(cStringIO.StringIO(data), header=0,index_col=0)
        if len(tempdf)==0:
            continue
        tempdf=tempdf[['bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total', 'price', 'quantity']]
        df = df.append(tempdf)
        
    
    if len(df)>0:
        #convert index to timeindex
        if type(df.index) != pd.tseries.index.DatetimeIndex:
            df.index = pd.to_datetime(df.index)       
        #clean and append to the H5 data store.        
        df['index'] = df.index    
        df = df.drop_duplicates()  
        del df['index']
        df = df.sort_index()   
        store = pd.HDFStore(hdf)
        store.append('dataframe', df, format='table',  complib='blosc', complevel=9, expectedrows=len(df))
        store.close()
        retval='and added to HDF5'

    #close opened files
    tar.close()   
    
    return retval
    
    
def zip_data_to_hdf_multi_ticker(directories, compresstype = 'bz2'):
            
    import time    
    j=0     
    start = time.time()
    for directory in directories:
        j=j+1
        tar_tickers_in_dir = getl.get_list_tar_tickers_in_dir(directory)
        i=0        
        for ticker in tar_tickers_in_dir:
            i=i+1
            ticker=ticker.replace('.tar','')
            retval=zip_data_to_hdf_single_ticker(ticker,directory, 'bz2') 
            print '%-8s:uncmpd %s, iter '%(ticker,retval) +str(i)+' of '+str(len(tar_tickers_in_dir))+ ', dir:'+str(j)+' of ' +str(len(directories)) + ' time=%5.2f' %((time.time()-start)/60)
    return
                
        
        
        

if __name__=='__main__':
    directories = ['D:\\Financial Data\Netfonds\\DailyTickDataPull\\Combined']
    zip_data_to_hdf_multi_ticker(directories,'bz2')
    print 'done'    

