from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

import pandas as pd
from random import randint
import time

# Enter the local path to your webdriver here

browser = webdriver.Firefox()

# Opens browser and loads Morningstar's ETF Database

browser.get('http://tools.morningstar.co.uk/uk/etfquickrank/default.aspx?Site=UK&Universe=ETALL%24%24ALL&LanguageId=en-GB')
# for NL: browser.get('http://tools.morningstar.nl/nl/etfquickrank/default.aspx?Site=NL&Universe=ETALL%24%24ALL&LanguageId=nl-NL')


time.sleep(randint(8, 12))

# Chooses institutional investor status

browser.switch_to.frame("frameContainer")
browser.find_element(By.LINK_TEXT, "Financial Professional").click()
# for NL: browser.find_element(By.LINK_TEXT, "Professionele belegger").click()
browser.switch_to.default_content()

time.sleep(randint(1, 4))

# Enter the universe for which you would like to extract data

universe = str(input("Enter universe\n"
                                   "NYSE Euronext - Amsterdam \n"
                                   "All Universes \n"
                                   "XETRA \n"
                                   "ETF - London Stock Exchange \n"
                                   "Borsa Italiana \n"
                                   "NASDAQ \n"
                                   "NASDAQ OMX \n"
                                   "NYSE ARCA \n"
                                   "SIX Swiss Exchange \n"
                                   "NYSE Euronext - Brussels \n"
                                   "NYSE Euronext - Paris \n"
                                   "Australia Stock Exchange \n"
                                   "Hong Kong Stock Exchange \n"
                                   "Madrid Stock Exchange \n"
                                   "Boerse Stuttgart \n"
                                   "Tokyo Stock Exchange \n"
                                   "Toronto Stock Exchange \n"
                                   "Singapore Exchange \n"
                                   "Stockholm \n"
                                   "US Composite \n"
                                   "Bolsa Mexicana de Valores \n"
                                   "Johannesburg Stock Exchange \n"
                                   "Bombay Stock Exchange \n"
                                   "Oslo Bors \n"
                                   "TASE \n"
                                   "Wiener Boerse \n \n"))

browser.find_element(By.ID, "ctl00_ContentPlaceHolder1_aFundQuickrankControl_ddlUniverse").click()
select = Select(browser.find_element_by_id('ctl00_ContentPlaceHolder1_aFundQuickrankControl_ddlUniverse'))
select.select_by_visible_text(universe)

time.sleep(randint(1, 4))

col1 = []
col2 = []
col3 = []
col4 = []
col5 = []
col6 = []

# Switches the view so that the max number of ETFs is shown (i.e. 500)

select = Select(browser.find_element_by_id('ctl00_ContentPlaceHolder1_aFundQuickrankControl_ddlPageSize'))
select.select_by_visible_text("500 per page")
# for NL: select.select_by_visible_text("500 per pagina")

time.sleep(randint(1, 4))

# Extracts data

keep_going = True

while keep_going:

    for etf in browser.find_elements_by_xpath("(//TD[@class='msDataText gridFundName Shrink'])"):
        try:
            name = etf.find_element_by_css_selector("a:first-of-type").text
            col1.append(name)

        except(TimeoutException, NoSuchElementException):
            col1.append("NA")

        try:
            link = etf.find_element_by_css_selector("a:first-of-type").get_attribute('href')
            col2.append(link)

        except(TimeoutException, NoSuchElementException):
            col2.append("NA")

    for ter in browser.find_elements_by_xpath("(//TD[@class='msDataNumeric gridOngoingCharge'])"):
        try:
            col3.append(ter.text)

        except(TimeoutException, NoSuchElementException):
            col3.append("NA")

    for close in browser.find_elements_by_xpath("(//TD[@class='msDataNumeric gridClosePrice'])"):
        try:
            col4.append(close.text)

        except(TimeoutException, NoSuchElementException):
            col4.append("NA")

    for item in browser.find_elements_by_xpath("(//TD[@class='msDataText gridStarRating'])"):
        try:
            stars = item.find_element_by_css_selector("img:first-of-type").get_attribute('src')
            col5.append(stars)

        except(TimeoutException, NoSuchElementException):
            col5.append("NA")

    for category in browser.find_elements_by_xpath("(//TD[@class='msDataText gridCategoryName Shrink'])"):
        try:
            col6.append(category.text)

        except(TimeoutException, NoSuchElementException):
            col6.append("NA")

    try:
        if browser.find_element_by_xpath("(//A[@class='pager_disabled disabled'][text()='Next'])"):
            keep_going = False

    except:
        v = browser.find_element(By.LINK_TEXT, "Next")
        # for NL: v = browser.find_element(By.LINK_TEXT, "Volgende")
        browser.execute_script('arguments[0].click()', v)
        time.sleep(randint(5, 8))

df = pd.DataFrame({'name': col1, 'link': col2, 'TER': col3, 'close': col4, 'stars': col5, 'category': col6})
browser.close()
df.to_csv("ETFdata-"+str(universe)+".csv", sep=';', encoding='utf-8')



