import pandas as pd
import pymongo
from pymongo import MongoClient
import time
from os import listdir, walk
from os.path import isfile, join
import re
import numpy as np
import json
import os
from pymongo.errors import BulkWriteError
from pprint import pprint

#rename files in archives
def rename_elements_in_archives(file_path, slash):
    import zipfile
    
    filenames = [f for f in listdir(file_path) if isfile(join(file_path, f))]
    i = 0    
    
    for filename_ in filenames:
        print i
        try:
            if filename_.lower().endswith('.zip'):
                #for renaming in subdirectories
                if slash:
                    print "Renaming "+file_path+"\\"+filename_
                    zipped = zipfile.ZipFile(file_path+"\\"+filename_, 'r')
                    tmp = zipfile.ZipFile(file_path+"\\tmp_"+filename_, 'w')
            
                    for file in zipped.filelist:
                        tmp.writestr(filename_.replace(".", "")+file.filename, zipped.read(file.filename))
                    zipped.close()
                    tmp.close()
                #for initial, normal renaming
                else:
                    print "Renaming "+file_path+filename_
                    zipped = zipfile.ZipFile(file_path+filename_, 'r')
                    tmp = zipfile.ZipFile(file_path+"tmp_"+filename_, 'w')
            
                    for file in zipped.filelist:
                        tmp.writestr(filename_.replace(".", "")+file.filename, zipped.read(file.filename))
                    zipped.close()
                    tmp.close()
            i += 1
        except Exception as e:
            print e

#unzip all magic
def unzip(file_path, subdir):
    import zipfile
    
    #walk through folder and unzip all
    i = 0
    d = 0
    
    #if archives were inside archives
    if subdir:
        dirs = walk(file_path)
        
        for dir in dirs:
            if d > 0:
                print "Checking "+dir[0]
                rename_elements_in_archives(dir[0], True)
                filenames = [f for f in listdir(dir[0]) if isfile(join(dir[0], f))]
                for filename in filenames:
                    if "tmp" in filename:
                        try:
                            print i
                            print "Unzipping "+dir[0]+"\\"+filename
                            with zipfile.ZipFile(dir[0]+"\\"+filename, "r") as zipped:
                                zipped.extractall(dir[0]+"\\")
                            i += 1
                        except Exception as e:
                            print e
            d += 1
    #normal way
    else:
        filenames = [f for f in listdir(file_path) if isfile(join(file_path, f))]
    
        for filename in filenames:
            if "tmp" in filename:
                try:
                    print "Unzipping %s" %i
                    print filename
                    with zipfile.ZipFile(file_path+filename, "r") as zipped:
                        zipped.extractall(file_path)
                        i += 1
                except Exception as e:
                    print e


#collect all csv files
def collect_bones(file_path):
    lst = []
    
    #get all subdirectories
    dirs = walk(file_path)
    i = 0
    
    for dir in dirs:
        print "Checking "+dir[0]
        filenames = [f for f in listdir(dir[0]) if isfile(join(dir[0], f))]
        for filename_ in filenames:
            if filename_.lower().endswith('.csv'):
                lst.append(dir[0]+"\\"+filename_)
                print "Collected %s" %i
                i += 1    
    #return unique list
    return list(set(lst))


#collect all cleaned csv files
def collect_cleaned_bones(file_path):
    lst = []
    
    #get all subdirectories
    dirs = walk(file_path)
    i = 0
    
    for dir in dirs:
        print "Checking "+dir[0]
        filenames = [f for f in listdir(dir[0]) if isfile(join(dir[0], f))]
        for filename_ in filenames:
            if filename_.lower().endswith('_tmp.csv'):
                lst.append(dir[0]+"\\"+filename_)
                print "Collected %s" %i
                i += 1    
    #return unique list
    return list(set(lst))


#make symbol name
def clean_name(s):
    m = re.search(r'\D{3}_\D{3}', s)
    if m:
        name = str(m.group()).replace("_", "")
    return name
    

#cleancsv files from null bytes
def clean_csv(path_to_files):
    k = collect_bones(path_to_files)
    
    #do rewrite magic
    for s in range(0, len(k)):
        print "------------------------------"
        print k[s] 
        fle = open(k[s], 'rb')
        data = fle.read()
        fle.close()
        cleaned = open(k[s]+"_tmp.csv", 'wb')
        cleaned.write(data.replace('\x00', ''))
        cleaned.close()


#write collected list of files
def write_list(path_to_files):
    if os.path.isfile(path_to_files+'_symbol_list.txt'):
        pass
    else:
        print "There isn't such file, so we'll collect filenames into it."
        #collect files
        k = collect_cleaned_bones(path_to_files)
        with open(path_to_files+'_symbol_list.txt', 'w') as file_list:
            for ki in k:
                file_list.write(ki+"\r\n")
        file_list.close()


#connect
def connect_to_mongo(dbname):
    client = MongoClient()
    db = client[dbname]
    return db


#put data into mongoDB
def put_data(data, db, tableName):
    try:
                #add unique indexes
        db[tableName].create_index([('TID', pymongo.ASCENDING)], unique=True)
        db[tableName].create_index([('DATE_TIME', pymongo.ASCENDING)], unique=False)

        n = 0
        #get order of columns
        for c in data.columns:
            #get column number of datetime
            if(data[c].dtype == np.object):
                if len(data.ix[2][n]) > 18:
                    timecol = n
            
            #get column number of bud and ask
            if(data[c].dtype == np.float64): 
                bidcol = n-1
                askcol = n
                if data.ix[2][bidcol] < data.ix[2][askcol]:
                    bidcol = n-1
                    askcol = n
                else:
                    bidcol = None
                    askcol = None
                
            #check for tid column number
            if(data[c].dtype == np.int64):
                tidcol = n
            n += 1
                
        timerows = data[data.columns[timecol]]
        bidrows = data[data.columns[bidcol]]
        askrows = data[data.columns[askcol]]
        tidrows = data[data.columns[tidcol]]
            
        #full corrected dataframe
        corrected_form = pd.concat([timerows, bidrows, askrows, tidrows], axis=1, keys=["DATE_TIME", "BID", "ASK", "TID"])#, join_axes=[timerows])
            
        #set time series based index
        corrected_form.set_index(keys="DATE_TIME", inplace=False)
    
        #convert strings to pandas DateTimeIndex
        corrected_form.DATE_TIME = pd.to_datetime(corrected_form.DATE_TIME, format='%Y-%m-%d %H:%M:%S.%f')
    
        #try to insert at once
        rec = json.loads(corrected_form.T.to_json(date_format='iso8601', double_precision=6, date_unit='ns')).values()
        
        #db[tableName].insert(rec, continue_on_error=True)
        db[tableName].insert_many(rec, ordered=False, bypass_document_validation=False)
    except BulkWriteError as bwe:
        pprint(bwe) #.bwe.details

#script body
def main():

    scriptStart = time.time()
    
    #config
    path_to_files = os.path.abspath("D:/data/fx/")
    dbname = 'lean'
    
    #establish connection to mongo
    #client = MongoClient()    
    db = connect_to_mongo(dbname)
    
    '''
    #do all required stuff before dealing with dbase
    rename_elements_in_archives(path_to_files, False)
    unzip(path_to_files, False)
    unzip(path_to_files, True)
    clean_csv(path_to_files)
    '''
    
    write_list(path_to_files)
    
    m = 0
    
    with open(path_to_files+'_symbol_list.txt', 'r') as datfl:
        filelist = datfl.readlines()
        #print filelist
        
        for li in filelist:
            try:
                file_toparse = li[:-2] #clip new line
        
                print "------------------------------"
                print m
                m += 1
                print file_toparse
                tableName = clean_name(file_toparse)+"_ticks"
                print tableName
        
                #create collection
                rows = db[tableName]
        
                data = pd.read_csv(file_toparse, sep=',')
                
                #put data into Mongo
                put_data(data, db, tableName)
            
                #write new list of data files
                datfl.close()
                cnt = 0
                with open(path_to_files+'_symbol_list.txt', 'w') as datfl:
                    for ki in filelist[m:]:
                        datfl.write(ki)
                        cnt += 1
                datfl.close()
                
                datfl = open(path_to_files+'_symbol_list.txt', 'r')
                
                
                print "New length %s" %cnt
                print "Passed"
            
            except Exception as e:
                print e
        
    datfl.close()
    timeused = (time.time()-scriptStart)/60
    print "Done in %s minutes" %round(timeused,2) 
    
main()
