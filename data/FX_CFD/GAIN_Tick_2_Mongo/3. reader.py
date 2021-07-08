#read data
def read_mongoDB_from_ticks(dbase, collection, query={}, reSample=False, frequency_='H'):
    
    #query if needed
    cursor = db[collection].find(query).sort('DATE_TIME')
    
    #expand cursor and create dataframe
    df =  pd.DataFrame(list(cursor))
    
    #set index todate time column
    df.set_index(keys="DATE_TIME", inplace=True)
    
    #convert timestamp to strings
    df.index = pd.to_datetime(df.index)
    
    #delete useless
    del df['_id']
    del df['TID']
    
    #voila
    if reSample:
        return df.resample(frequency_).dropna()
    else:
        return df
