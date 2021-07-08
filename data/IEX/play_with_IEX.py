import config, json
from iex import IEXStock
from datetime import datetime, timedelta

symbol = st.sidebar.text_input("Symbol", value='MSFT')
stock = IEXStock(config.IEX_TOKEN, symbol)


company = stock.get_company_info()
json.dumps(company)
company['companyName']
company['industry']
company['description']


news = stock.get_company_news()
client.set(news_cache_key, json.dumps(news))

dt = datetime.utcfromtimestamp(article['datetime']/1000).isoformat()
print(f"Posted by {article['source']} at {dt}")
article['url']
article['summary']
        
stats = stock.get_stats()
stats['peRatio']
stats['forwardPERatio']
stats['pegRatio']
stats['priceToSales']
stats['priceToBook']

stats['revenue']
stats['totalCash']
stats['currentDebt']
stats['day200MovingAvg']
stats['day50MovingAvg']

fundamentals = stock.get_fundamentals('quarterly')
for quarter in fundamentals:
    quarter['filingDate']
    quarter['revenue']
    quarter['incomeNet']
    
dividends = stock.get_dividends()
json.dumps(dividends)
for dividend in dividends:
    dividend['paymentDate']
    dividend['amount']

institutional_ownership = stock.get_institutional_ownership()
json.dumps(institutional_ownership)
for institution in institutional_ownership:
    institution['date']
    institution['entityProperName']
    institution['reportedHolding']

insider_transactions = stock.get_insider_transactions()
json.dumps(insider_transactions)
for transaction in insider_transactions:
    transaction['filingDate']
    transaction['fullName']
    transaction['transactionShares']
    transaction['transactionPrice']
