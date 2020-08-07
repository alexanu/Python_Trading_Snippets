# Source: https://github.com/philipoedi/etf_scraper

from selenium import webdriver
from lxml import html
import pandas as pd
import time
import datetime

#get username and pw
user_in = input("enter username:\n")
pass_in = input("enter pw:\n")


#Start browser
print("starting browser\n")

browser = webdriver.Firefox() #replace with .Firefox(), or with the browser of your choice
time.sleep(3)
login = "https://www.etf.com/user/login"
url = "http://www.etf.com/etfanalytics/etf-finder"
browser.get(login) #navigate to the login page
time.sleep(3)

#login
username = browser.find_elements_by_id("edit-name")
password = browser.find_elements_by_id("edit-pass")

username[0].send_keys(user_in) 
time.sleep(3)
password[0].send_keys(pass_in) 
time.sleep(3)
submitButton = browser.find_element_by_id("user_login") 
submitButton.click()
time.sleep(3)

#change to etf-finder
browser.get(url)

morePages = browser.find_element_by_id("show100")
morePagesButton = morePages.find_element_by_id("inactiveResult")
morePagesButton.click()
time.sleep(5)

pages = browser.find_elements_by_id("totalPages")
pages = int(pages[0].text.split(" ")[1])

#browser.get("http://www.etf.com/etfanalytics/etf-finder") #navigate to page behind login
innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a string
innerHTML2 = innerHTML.encode('ascii', 'xmlcharrefreplace')
htmlElem = html.document_fromstring(innerHTML2) #make HTML element object

innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a string
innerHTML2 = innerHTML.encode('ascii', 'xmlcharrefreplace')
htmlElem = html.document_fromstring(innerHTML2) #make HTML element object
    
table = htmlElem.cssselect("table")[8]
header = table.cssselect("th")

columns = [col.text_content() for col in header]

elements_list= []
elements_sub_list = []

print("getting data\n")
#while True:
for i in range(pages):    
    innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a string
    innerHTML2 = innerHTML.encode('ascii', 'xmlcharrefreplace')
    htmlElem = html.document_fromstring(innerHTML2) #make HTML element object
    
    table = htmlElem.cssselect("table")[8]
    
    elements = table.cssselect("td")
    
    num = 1
    for ele in elements:
        if (num)%len(columns) != 0:
            elements_sub_list.append(ele.text_content())
        else:
            elements_sub_list.append(ele.text_content())
            elements_list.append(elements_sub_list)
            elements_sub_list = []
        num += 1    
                
    nextButton = browser.find_element_by_id("nextPage") 
    try: 
        nextButton.click()    
    except:
        break
    time.sleep(2)
    

dataframe = pd.DataFrame(elements_list).dropna(axis=1)
dataframe.columns = columns


for i,AUM_string in enumerate(dataframe["AUM"]):
    try:
        AUM_value = float(AUM_string[1:-1])
    except:
        continue
    AUM_unit = AUM_string[-1]
    if AUM_unit == "M":
        dataframe.AUM[i] = AUM_value
        continue
    if AUM_unit == "K":
        dataframe.AUM[i] = AUM_value / 1000.
        continue
    if AUM_unit == "B":
        dataframe.AUM[i] = AUM_value * 1000.
        continue
    else:
        continue
    
for col in columns:
    sub_data = dataframe[col]
    condition = sub_data.str.contains("--")
    sub_data[condition == True] = 0 

print("writing data\n")
today = datetime.date.today()
filename = str(today.year)+"_"+str(today.month)+"_"+str(today.day)+"_etf_database"
dataframe.to_excel("%s.xlsx" % filename)
print("finished")
