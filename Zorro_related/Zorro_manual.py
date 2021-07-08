#Checking the chapters of Zorro

from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as bs # BS parses HTML into an easy machine readable tree format to extract DOM Elements quickly. 
import pandas as pd


directory = 'c:\\Users\\oanuf\\GitHub\\Fun\\'
zorro_content = "http://www.zorro-trader.com/manual/ht_contents.htm"
page = urlopen(zorro_content)
soup = bs(page)
zorro_pages_links = [a.get('href') for a in soup.find_all('a', href=True)]
zorro_pages_names = [a.get('title') for a in soup.find_all('a')]
df = pd.DataFrame({'Names':zorro_pages_names,
				    'Link':zorro_pages_links})
df["Link"] = 'https://www.zorro-trader.com/manual/' + df["Link"].astype(str)
df.to_csv(directory+"Zorro_content.csv", # creating new file
			sep=";", 
			index=False) # we need to eliminate the index column
