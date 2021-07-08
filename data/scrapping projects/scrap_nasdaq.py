def scrap_nasdaq(*url):
    from urllib.request import Request, urlopen
    from bs4 import BeautifulSoup
    import pandas as pd
    import time
    from datetime import datetime 

    start_time = time.time()

    # if tuple is not empty 
    if url:
        nasdaq_url = url[0]
    # else use default url
    else:
        # input hard coded url here
        nasdaq_url = "http://www.nasdaq.com/extended-trading/premarket-mostactive.aspx"
    # mark up
    page = urlopen(nasdaq_url)

    # collect all the text data in a list
    text_data = []
    soup = BeautifulSoup(page, "html.parser")
    # search all html lines containing table data about stock 
##    html_data = soup.find_all('div', id="_advanced")
##    for row in html_data:
##        print("here")
##        print(row.get_text().rstrip())
##        text_data.append(row.get_text())
##
    div_data = soup.find_all('div', id="_advanced")
    tbody_data = []
    # find table data in html_data 
    for elem in div_data:
        tbody_data = elem.find_all('td')
    stocks = []        
    for stock in tbody_data:
        stocks.append(stock.get_text())

    # takes as input stock array and outputs list of lists
    # each list contains information about one stock
    def helper(data):
        counter = 0 
        list_of_lists = []
        temp_list = []
        for elem in data:
            # end of each stock
            if counter % 6 == 0:
                # add currently filled temp_list to list_of_lists if not empty
                if temp_list: 
                    list_of_lists.append(temp_list)
                # reset to empty list
                temp_list = []
            temp_list.append(elem)
            counter += 1
        list_of_lists.append(temp_list)
        return list_of_lists

    stock_data = helper(stocks)
    # clean up data 
    stock_data = list(map(lambda x: [x[0].strip(), float(x[3][2:]), x[4].replace(u'\xa0▲\xa0', u' , ')], stock_data))

    # returns list of lists
    # each list contains: ticker, price (float), % change (float)
    stock_data = list(map(lambda x: [x[0], x[1], float(x[2].split(',')[1][:-2])], stock_data))

    # helper to filter 0.5 < price < 5, and % change > 8.0 (temporary)
    def helper(lst):
        return lst[1] > 0.5 and lst[1] < 5 and lst[2] > 8.0
    
    return list(map(lambda x: x[0], filter(lambda x: x if helper(x) else None,stock_data)))



    




