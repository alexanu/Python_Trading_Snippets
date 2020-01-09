# Source: https://github.com/sjev/trading-with-python/blob/master/lib/extra.py



def getSpyHoldings(dataDir):
    ''' get SPY holdings from the net, uses temp data storage to save xls file '''

    import urllib.request, urllib.parse, urllib.error
    
    dest = os.path.join(dataDir,"spy_holdings.xls")
    
    if os.path.exists(dest):
        print('File found, skipping download')
    else:
        print('saving to', dest)
        urllib.request.urlretrieve ("https://www.spdrs.com/site-content/xls/SPY_All_Holdings.xls?fund=SPY&docname=All+Holdings&onyx_code1=1286&onyx_code2=1700",
                             dest) # download xls file and save it to data directory
        
    # parse
    import xlrd # module for excel file reading
    wb = xlrd.open_workbook(dest) # open xls file, create a workbook
    sh = wb.sheet_by_index(0) # select first sheet
    
    
    data = {'name':[], 'symbol':[], 'weight':[],'sector':[]}
    for rowNr  in range(5,505): # cycle through the rows
        v = sh.row_values(rowNr) # get all row values
        data['name'].append(v[0])
        data['symbol'].append(v[1]) # symbol is in the second column, append it to the list
        data['weight'].append(float(v[2]))
        data['sector'].append(v[3])
      
    return  pd.DataFrame(data)    
    