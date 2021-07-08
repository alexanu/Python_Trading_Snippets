# Source: https://github.com/swang2016/benzinga_movers_scraper/blob/master/scripts/scrape_benzinga.py




def get_earnings_dates(tcks, start_date): #gets earnings report dates given list of tickers and start date
    start_date = pd.to_datetime(start_date)
    fd = pd.DataFrame({'filing_dates':[], 'ticker':[]})
    tcks = set(tcks)
    for t in tcks:
        ten_k_end_reached = 0
        ten_q_end_reached = 0
        try: #get 10Ks
            filing_page = 1
            while ten_k_end_reached == 0:
                url = 'https://www.marketwatch.com/investing/stock/' + t + '/secfilings?seqid=' + str(filing_page) + '&subview=10K'
                page = requests.get(url)
                soup = BeautifulSoup(page.content, "html.parser")

                table = soup.find('table', id = 'Table2')

                if len(soup.find_all('b', text = re.compile('Sorry, there are no.*'))) > 0:
                    break #end of marketwatch filing pages

                rows = table.find_all('tr')
                filing_dates = []

                filing_page += 20 #increment filings page

                for i in range(1, len(rows) - 1):
                    row = rows[i]
                    fdt = row.find_next('td').text.strip()
                    if pd.to_datetime(fdt) <= start_date:
                        ten_k_end_reached = 1
                        break
                    else:
                        filing_dates.append(fdt)
                temp = pd.DataFrame({'filing_dates':filing_dates, 'ticker':t})

                fd = pd.concat([fd, temp], axis = 0)
        except: 
            print('could not find 10K filing dates for:', t)
        try: #get 10Qs
            filing_page = 1
            while ten_q_end_reached == 0:
                url = 'https://www.marketwatch.com/investing/stock/' + t + '/secfilings?seqid=' + str(filing_page) + '&subview=10Q'
                page = requests.get(url)
                soup = BeautifulSoup(page.content, "html.parser")

                table = soup.find('table', id = 'Table2')

                if len(soup.find_all('b', text = re.compile('Sorry, there are no.*'))) > 0:
                    break #end of marketwatch filing pages

                rows = table.find_all('tr')
                filing_dates = []

                filing_page += 20 #increment filings page

                for i in range(1, len(rows) - 1):
                    row = rows[i]
                    fdt = row.find_next('td').text.strip()
                    if pd.to_datetime(fdt) <= start_date:
                        ten_q_end_reached = 1
                        break
                    else:
                        filing_dates.append(fdt)
                temp = pd.DataFrame({'filing_dates':filing_dates, 'ticker':t})

                fd = pd.concat([fd, temp], axis = 0)
        except:
            print('could not find 10Q filing dates for:', t)
    fd['filing_dates'] = pd.to_datetime(fd.filing_dates)
    fd = fd.sort_values(['ticker', 'filing_dates'], ascending = False)
    fd = fd.drop_duplicates()
    return fd



def get_new_earnings_dates(tcks, earnings, start_date): #updates earnings reports dates given list of tickers, old earnings date data, and start date
    start_date = pd.to_datetime(start_date)
    fd = pd.DataFrame({'filing_dates':[], 'ticker':[]})
    tcks = set(tcks)

    for t in tcks:
        if t not in set(earnings.ticker): #no filing date saved
            temp = get_earnings_dates(set([t]), start_date)
            fd = pd.concat([fd, temp], axis = 0)
        else: #filing dates for ticker saved, update with new dates
            update_from = earnings[earnings.ticker == t].filing_dates.max()
            temp = get_earnings_dates(set([t]), update_from)
            fd = pd.concat([fd, temp], axis = 0)  
            
    fd = fd.drop_duplicates()
    return fd
