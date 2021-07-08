# Scrape all the descriptions of ETF from etf.com
# ETF.com has really nice description of ETFs.
# I didn't manage to scrape it simply with request due to scrapping protection
# Hence: Selenium

from selenium import webdriver
import pandas as pd
import time

ETF_directory= "C:\\Users\\...\\Tkrs_metadata\\Tkrs\\ETF\\"
US_ETF=pd.read_excel(ETF_directory + 'ETFDB.xlsx',sheet_name='US_ETFs',skiprows=2) 
US_ETF_list=US_ETF[~(US_ETF["xx"] == 'Old')].CompositeSymbol.to_list()
US_ETF_list=US_ETF[US_ETF["No_desc"] == 'Check'].CompositeSymbol.to_list()

start = time.time()
browser = webdriver.Firefox() #replace with .Firefox(), or with the browser of your choice
time.sleep(6)
ETF_descr=[]
for ETF in US_ETF_list:
    url = 'https://www.etf.com/'+ETF+'#overview' # important to add #overview
    try:
        browser.get(url)
        time.sleep(9) # I first put 3s, but many ETFs didn't manage to load
        long_html=browser.find_element_by_xpath('/html/body/div[7]/section/div/div/div[3]/div[1]/div[1]/section[2]/p[2]')
        short_html=browser.find_element_by_xpath('/html/body/div[7]/section/div/div/div[3]/div[1]/div[1]/section[1]/p')        
        ETF_description=short_html.text+long_html.text
        ETF_descr.append([ETF,ETF_description])
    except:
        continue

end = time.time()
delta = end - start
ETF_descr_df =pd.DataFrame(ETF_descr,columns = ['ETF','ETF_description']) # transform list of lists into panda
ETF_descr_df.to_csv('ETF_dot_com_descr.csv',mode='a',header=False)

browser.quit()