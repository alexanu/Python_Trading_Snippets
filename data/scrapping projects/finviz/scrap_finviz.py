def scrap_finviz(strategyNum, *url):
    from urllib.request import urlopen
    from bs4 import BeautifulSoup
    import pandas as pd
    import time
    from datetime import datetime 

    start_time = time.time()

    # if tuple is not empty 
    if url:
        finviz_url = url[0]
        page = urlopen(finviz_url)
    # else use default url
    else:
        # input hard coded url here
        finviz_url = "http://finviz.com/screener.ashx?v=111&s=ta_topgainers&f=sh_curvol_o500,sh_price_1to20"
        page = urlopen(finviz_url)

    hasNextPage = True
    firstPage = True
    # from page 2 onwards 
    currentPageIndex = 0
    # collect all the text data in a list
    text_data = []

    while hasNextPage: 
        if not firstPage:
            finviz_url += "&r=" + str(currentPageIndex)
            page = urlopen(finviz_url)
        soup = BeautifulSoup(page, "html.parser")

        # search all html lines containing table data about stock 
        html_data = soup.find_all('td', class_="screener-body-table-nw")
        counter = 0 
        for row in html_data:
            counter+= 1
            text_data.append(row.get_text())

        # toggle flag such that firstPage will be false after first run 
        firstPage = False

        # advance to next page url
        if currentPageIndex == 0:
            currentPageIndex += 21
        else:
            currentPageIndex += 20 
        # 220 is derrived from 20 stocks in a single page * 11 columns
        # anything lesser than 220 implies that page is not full (i.e. no next page)
        if counter < 220:
            hasNextPage = False       

    # takes as input text_data array and outputs list of lists
    # each list contains information about one stock
    def helper(data):
        counter = 0 
        list_of_lists = []
        temp_list = []
        for elem in data:
            # end of each stock
            if counter % 11 == 0:
                # add currently filled temp_list to list_of_lists if not empty
                if temp_list: 
                    list_of_lists.append(temp_list)
                # reset to empty list
                temp_list = []
            # change from string to int for % change
            if elem[-1] == '%':
                elem = float(elem[:-1])
            temp_list.append(elem)
            counter += 1
        list_of_lists.append(temp_list)
        return list_of_lists
    
    stock_data = helper(text_data)
    
    # remove the numerical index from each list
    for each_stock in stock_data:
        del each_stock[0]
        
    labels = ['Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Price', '% Change', 'Volume']
    df = pd.DataFrame.from_records(stock_data, columns=labels)
    # date of retrieval
    print(str(datetime.now())) 
    # time taken to retrieve data
    print('Time taken to draw data: ' + str(round(time.time() - start_time, 2)) + ' seconds')
    # save as csv file
    df.to_csv('/Users/ZengHou/Desktop/Stock Strategies/finviz_data_strategy' + str(strategyNum) + '.csv', index=False)
    return df


    




