# -*- coding: utf-8 -*-

import os
import re

def get_list_tickers_in_dir(directory=None):
    """
    gets the list of tickers in the directory
    """
    start_dir = os.getcwd()

    if directory == None:
        directory = start_dir
        
    listdir=os.listdir(directory)
    TCKRS=[]
    for ls in listdir:
#        if not ls.endswith('.csv'):
#            continue
        
        check = re.match("[A-Z]*?\.[A-Z].",ls)
        if check==None:
            continue
        TCKRS.append(check.group()[:-1])#we do not wan to include the last '.' in TCKRS
    
    #remove duplicates
    TCKRS = list(set(TCKRS))
    os.chdir(start_dir)
    return (TCKRS, listdir)    
    
def get_list_tar_tickers_in_dir(directory):
    """
    get the list of tickers that have been archived to TAR files
    """
    start_dir = os.getcwd()

    if directory == None:
        directory = start_dir
        
    listdir=os.listdir(directory)
    TCKRS=[]
    for ls in listdir:
#        if not ls.endswith('.csv'):
#            continue
        
        check = re.match("[A-Z]*?\.[A-Z].tar",ls)
        if check==None:
            continue
        TCKRS.append(check.group())
    
    #remove duplicates
    TCKRS = list(set(TCKRS))
    os.chdir(start_dir)
    return TCKRS       
    
    
    
def get_csv_file_list(TCKR,listdir, directory=None):
    """
    Input: single ticker in format 'TICKER.X', where X is netfonds exchange letter (N:NYSE,O:NASDAQ,A:AMEX)
    Returns the list of files with ticker=TCKR
    """   
    start_dir = os.getcwd() #save start dir so we can revert back at the end of program
    if directory==None:
        directory = start_dir
    
    os.chdir(directory)
    
    #enables not entering valid listdir
    if listdir==0:
        listdir = os.listdir(directory)
        
    ls = str(listdir)

        
    #search for single run files    
    check = re.search('\''+TCKR+'\.[0-9]*\.csv\', ',ls)
    if check == None:
        print 'Error: ticker='+TCKR+' single date csv not found in:'
        print directory
        return 'no tickers'
    files=[]
    while check !=None:
        s = str(check.group())
        s=s.replace("'","")
        s=s.replace(", ","")
        files.append(s)
        ls=ls.replace(check.group(),'xxxxx, ',1)  #replace 1st instance of filename wit xxxxx          
        check = re.search("\'"+TCKR+"\.[0-9]*\.csv\', ",ls)     
    
    #search for combined files
    check = re.search('\''+TCKR+'\.combined.csv\', ',ls)
    if check !=None:
        s = str(check.group())
        s=s.replace("'","")
        s=s.replace(", ","")
        files.append(s)

    os.chdir(start_dir)
    return files