# Import libraries
import pandas as pd
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
from urllib.request import urlopen, Request
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Parameters 
n = 3 #the # of article headlines displayed per ticker
tickers = ['AAPL', 'TSLA', 'AMZN']

# Get Data
finviz_url = 'https://finviz.com/quote.ashx?t='
news_tables = {}

for ticker in tickers:
    url = finviz_url + ticker
    req = Request(url=url,headers={'user-agent': 'my-app/0.0.1'}) 
    resp = urlopen(req)    
    html = BeautifulSoup(resp, features="lxml")
    news_table = html.find(id='news-table')
    news_tables[ticker] = news_table

try:
    for ticker in tickers:
        df = news_tables[ticker]
        df_tr = df.findAll('tr')
    
        print ('\n')
        print ('Recent News Headlines for {}: '.format(ticker))
        
        for i, table_row in enumerate(df_tr):
            a_text = table_row.a.text
            td_text = table_row.td.text
            td_text = td_text.strip()
            print(a_text,'(',td_text,')')
            if i == n-1:
                break
except KeyError:
    pass

# Iterate through the news
parsed_news = []
for file_name, news_table in news_tables.items():
    for x in news_table.findAll('tr'):
        text = x.a.get_text() 
        date_scrape = x.td.text.split()

        if len(date_scrape) == 1:
            time = date_scrape[0]
            
        else:
            date = date_scrape[0]
            time = date_scrape[1]

        ticker = file_name.split('_')[0]
        
        parsed_news.append([ticker, date, time, text])
        
# Sentiment Analysis
analyzer = SentimentIntensityAnalyzer()

columns = ['Ticker', 'Date', 'Time', 'Headline']
news = pd.DataFrame(parsed_news, columns=columns)
scores = news['Headline'].apply(analyzer.polarity_scores).tolist()

df_scores = pd.DataFrame(scores)
news = news.join(df_scores, rsuffix='_right')

# View Data 
news['Date'] = pd.to_datetime(news.Date).dt.date

unique_ticker = news['Ticker'].unique().tolist()
news_dict = {name: news.loc[news['Ticker'] == name] for name in unique_ticker}

values = []
for ticker in tickers: 
    dataframe = news_dict[ticker]
    dataframe = dataframe.set_index('Ticker')
    dataframe = dataframe.drop(columns = ['Headline'])
    print ('\n')
    print (dataframe.head())
    
    mean = round(dataframe['compound'].mean(), 2)
    values.append(mean)
    
df = pd.DataFrame(list(zip(tickers, values)), columns =['Ticker', 'Mean Sentiment']) 
df = df.set_index('Ticker')
df = df.sort_values('Mean Sentiment', ascending=False)
print ('\n')
print (df)


#-------------------------------------------------------------------------------------
from textblob import TextBlob
import numpy as np
import requests
import pandas as pd
import nltk
demo = 'your api key'
#nltk.download('punkt')
company = 'AAPL'

transcript = requests.get(f'https://financialmodelingprep.com/api/v3/earning_call_transcript/{company}?quarter=3&year=2020&apikey={demo}').json()

transcript = transcript[0]['content']
print(transcript)

sentiment_call = TextBlob(transcript)

print(sentiment_call.sentiment)
print(sentiment_call.sentences)

sentiment_call.sentences
negative = 0
positive = 0
neutral = 0
all_sentences = []

for sentence in sentiment_call.sentences:
  #print(sentence.sentiment.polarity)
  if sentence.sentiment.polarity < 0:
    negative +=1
  if sentence.sentiment.polarity > 0:
    positive += 1
  else:
    neutral += 1
 
  all_sentences.append(sentence.sentiment.polarity) 

print('positive: ' +  str(positive))
print('negative: ' +  str(negative))
print('neutral: ' + str(neutral))

all_sentences = np.array(all_sentences)
print('sentence polarity: ' + str(all_sentences.mean()))

for sentence in sentiment_call.sentences:
  if sentence.sentiment.polarity > 0.8:
     print(sentence)