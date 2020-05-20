# -*- coding: utf-8 -*-


import tarfile
import gzip
import bz2
import os
import sys
import StringIO
import time   
import itertools
import multiprocessing
import get_lists as getl
import HDFcreate as HDFcreate


def compress_tickers_parallel(tickers=None, directories=None, compression='bz2', complevel=9, n_process=1):
    """
    mode 1: Tickers is list of individual tickers to compress, directories is corresponding directories.
            If directories is a single direectory, all tickers are in that directory
    mode 2: Tickers=None and directories is a list of directories. 
            All files in directories are compressed
    """
    
    
    """
    Step 1: Get list of tickers
    """
    #check tickers argument
    if tickers!=None:
        assert type(tickers)==list
        assert type(directories)==list        
        TICKERs=tickers
        if len(directories)==1:
            dir_=directories*len(tickers)
        listdir = 0
        
    elif directories!=None:
        assert type(directories)==list
        TICKERs=[] #list of tickers
        listdir=0
        dir_=[] #directories for each ticker in TICKERs
        for directory in directories:
            tickers_, listdir_ = getl.get_list_tickers_in_dir(directory)
            TICKERs.extend(tickers_)
#            listdir = listdir.extend(listdir_)
            dir_.extend([directory]*len(tickers_))         
    
    #create dictionary {ticker:directory}
    dirs = dict(itertools.izip(TICKERs,dir_))
    


    """
    Allocate workload to each process
    """    
    #allocate tickers to different processes
    length = len(TICKERs)
    index=[]
    ls_list=[]
    for i in range(n_process):
        index.append(range(i,length, n_process))  #indicies for each process
        ls = [TICKERs[x] for x in index[i]] #tickers corresponding to indicies
        ls_list.append(ls) #list of lists
    
    assert len(ls_list) == n_process
       
        
    """
    run the (multiple) process(es)
    """
    start = time.time()
    if n_process==1: #single process instance
        tickers=ls_list[0]
        compress_data_multi_tickers(tickers,dirs,start, compression,complevel)
    else: #initiate seperate processes to combine dates
        jobs=[]
        
        for tickers in ls_list:
            p = multiprocessing.Process(target=compress_data_multi_tickers, args=(tickers,dirs,start,compression,complevel))
            jobs.append(p)
            p.start()
            
        for j in jobs:
            j.join()   

        print 'Joined other threads'      
    
    return
    
    
def compress_data_multi_tickers(TICKERs, dirs, start, compression='bz2', complevel=9, supress='yes'):

    i=0      
    pName = multiprocessing.current_process().name
    listdir=0 #get the listdir within the single ticker file
    for ticker in TICKERs:
            i=i+1
            directory=dirs[ticker]
            
            if supress=='yes': #suppress printing to stdout
                sys.stdout = StringIO.StringIO()
                
            string = compress_data_single_ticker(ticker, listdir, directory,compression,complevel) 
            
            if supress=='yes': #switch printing to stdout back on
                sys.stdout = sys.__stdout__
            
            if type(string)!=str:               
                string=''
                
            print '%s: %-8s:%s compressed, iter=%-4.0d of %-4.0d,  time=%5.2f'%(pName,ticker,string,i,len(TICKERs),((time.time()-start)/60)) 
            sys.stdout.flush()
            
    return
        
    
    
def compress_data_single_ticker(TCKR, listdir, directory, compression='bz2', complevel=9):
    start_dir = os.getcwd()
    os.chdir(directory)
    
    if listdir==0:
        listdir=os.listdir(directory)
        
    
    #get list of files for ticker
    files_for_ticker = getl.get_csv_file_list(TCKR, listdir, directory)
    if (files_for_ticker==0):
        sys.stdout.flush()
        return 0
    if (files_for_ticker=='no tickers'):
        print TCKR + ': no files to archive in directory:' +directory
        string ='none '
        sys.stdout.flush()
        return string
    if len(files_for_ticker)==0:
        print TCKR + ': no files to archive in directory:' +directory
        string ='none '        
        sys.stdout.flush()
        return string

    #check if hdf file exists for this ticker    
#    hdf = TCKR+'.combined.h5'
#    if not(os.path.isfile(hdf)):
##        HDFcreate.create(TCKR, directory)        
#        print TCKR+': no hdf5 file in:'+directory
#        print 'could not archive any .csv files for:' +TCKR
#        print 'create hdf5 file first with tick_data_combine'
#        print 'program terminated'
#        f = open('D:\\Financial Data\\Netfonds\\DailyTickDataPull\\errorlog.txt','a')
#        f.write('%-8s: no hdf5 file in directory:%s \n'%(TCKR,directory))
#        f.close()
#        sys.stdout.flush()
#        return 0
        
    
    #remove combined files #and files not merged into h5 file
    for fl in files_for_ticker:
        if 'combined' in fl:
            files_for_ticker.remove(fl)

    if len(files_for_ticker)==0:
        print TCKR+': no .csv files exist ' 
        sys.stdout.flush()
        string ='none ' 
        return string
        
    #open the tarball    
    tar = tarfile.open(TCKR+'.tar','a')
    already_archived_files = tar.getnames()  
    
    #remove names already in the tarball
    for fl in already_archived_files: 
        if fl.replace('.'+compression,'') in files_for_ticker:
            os.remove(fl.replace('.'+compression,''))
            files_for_ticker.remove(fl.replace('.'+compression,''))
            
        
    #loop through remainng files and compress 
    comp_files=[]    
    for fl in files_for_ticker:
        outf = fl+'.'+compression      
        inf = open(fl, 'rb')
        data = inf.read()
        inf.close()
        
        if compression =='gz':
            fcomp = gzip.open(outf, 'wb', compresslevel=complevel) 
        elif compression =='bz2':
            fcomp = bz2.BZ2File(outf, 'wb', compresslevel=complevel)
       
        fcomp.write(data)
        fcomp.close()  
        #create list of compressed files for adding to tarball
        comp_files.append(outf)          

    for fl in files_for_ticker:
        os.remove(fl)
        
    #loop through compressed files and add to tarball
    i=0
    for fl in comp_files:
        if (fl in tar.getnames()): #check if its already in there
            print fl+': is already in tarball'
            f = open('D:\\Financial Data\\Netfonds\\DailyTickDataPull\\errorlog.txt','a')
            f.write('%-8s: already in TAR archive:%s \n'%(fl,directory))
            f.close()
            sys.stdout.flush()
            continue
        tar.add(fl)
        i+=1
    tar.close()
    #remove compressed files outside of archive
    for fl in comp_files:
        os.remove(fl)
        
    os.chdir(start_dir)
    sys.stdout.flush()
    return str(i)


        
if __name__ == '__main__':
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')

    directories = ['D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined\\ETF']
    compress_tickers_parallel(tickers=None, directories=directories, compression='bz2', complevel=9, n_process=3)


