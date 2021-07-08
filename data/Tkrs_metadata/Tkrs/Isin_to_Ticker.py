# Source: https://github.com/heungry/shortsellcollector/blob/master/dataReader.py

def mapISINtoTicker(ISIN):
    '''
    Send an collection of mapping jobs to the API in order to obtain the associated ticker(s).
    Parameters
    ISIN (list): 
        A list of ISIN that will be transformed to the OpenFIGI API request structure.
        
        
    Returns
    tickers (list):
        A list of tickers corresponding to the ISIN list.
    names (list):
        A list of names corresponding to the ISIN list.
        
    '''

    # Basic parameters of the FIGI API
    # See https://www.openfigi.com/api for more information.
    openfigi_url = 'https://api.openfigi.com/v2/mapping'
    openfigi_headers = {'Content-Type': 'text/json'}
    openfigi_headers['X-OPENFIGI-APIKEY'] = '7ae877e8-5c00-4464-b70a-9af7e49c9a19'
    
    # The mapping jobs per request is limited to 100
    tickers = []
    names = []
    slc = 100
    ISINs = [ISIN[i: i + slc] for i in range(len(ISIN))[::slc]]
    for isin in ISINs:
        jobs = [{'idType': 'ID_ISIN',
                    'idValue': id,
                    'exchCode':'GY', # XETRA
                    'securityType': 'Common Stock'} for id in isin]
        
        response = requests.post(url=openfigi_url, headers=openfigi_headers, json=jobs)
        if response.status_code != 200:
            raise Exception('Bad response code {}'.format(str(response.status_code)))

        tickers += [
            None if result.get('data') is None else result.get('data')[0]['ticker']\
            for result in response.json()]
        
        names += [
            None if result.get('data') is None else result.get('data')[0]['name']\
            for result in response.json()]

    num_none = sum([ticker is None for ticker in tickers])
    # Update logging
    logging = "=" * 50 + "\n" + str(dt.now()) + ": {} ISIN(s) find no tickers or names from FIGI API.\n".format(num_none) 
    logging += "=" * 50 + "\n\n"
    with open("logfile", "a") as f:
        f.write(logging)
    
    return tickers, names