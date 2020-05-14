
# Source: https://github.com/s0h3ck/finnhub-api-python-client

from finnhub import client as Finnhub

client = Finnhub.Client(api_key="********************")

# Get general information of a company 
client.company_profile(symbol="NFLX")

# Get latest company's CEO compensation
client.ceo_compensation(symbol="NFLX")

# Get latest analyst recommendation trends
client.recommendation(symbol="NFLX")

# Get latest price target consensus
client.price_target(symbol="NFLX")

# Get latest stock upgrade and downgrade
client.upgrade_downgrade(symbol="NFLX")

# Get company option chain
client.option_chain(symbol="NFLX")

# Get company peers
client.peers(symbol="NFLX")

# Get company quarterly earnings
client.earnings(symbol="NFLX")

# List supported stock exchanges
client.exchange()

# List supported stocks
client.stock_symbol(exchange="US")

# Get quote data
client.quote(symbol="NFLX")

# Get candlestick data for stocks
client.stock_candle(symbol="NFLX", resolution="D", count=200)
client.stock_candle(symbol="NFLX", resolution="D", **{'from':'1575968404', 'to': '1575968424'})

# [PREMIUM] Get tick data
# client.stock_tick(symbol="NFLX", resolution="D", **{'from':'1575968404', 'to': '1575968424'})

# List supported forex exchanges
client.forex_exchange()

# List supported forex symbols
client.forex_symbol(exchange="oanda")

# Get candlestick data for forex symbols
client.forex_candle(symbol="OANDA:EUR_USD", resolution="D", count=200)

# List supported crypto symbols by exchange
client.crypto_symbol(exchange="binance")

# Get candlestick data for crypto symbols
client.crypto_candle(symbol="BINANCE:BTCUSDT", resolution="D", count=200)

# Get pattern recognition on a symbol (support double top/bottom, triple top/bottom, head and shoulders, triangle, wedge, channel, flag, and candlestick patterns)
client.scan_pattern(symbol="NFLX", resolution="D")

# Get support and resistance levels for a symbol
client.scan_support_resistance(symbol="NFLX", resolution="D")

# Get aggregate signal of multiple technical indicators
client.scan_technical_indicator(symbol="NFLX", resolution="D")

# Get latest market news
client.news(category="general")

# List latest company news by symbol
client.company_news(symbol="NFLX")

# Get company's news sentiment and statistics
client.news_sentiment(symbol="NFLX")

# List countries where merger and acquisitions take place
client.merger_country()

# List latest merger and acquisitions deal by country.
client.merger(country="United States")

# List codes of supported economic data
client.economic_code()

# Get economic data
client.economic(code="MA-USA-G")

# Get recent and coming economic releases
client.calendar_economic()

# Get recent and coming earnings release
client.calendar_earnings()

# Get recent and coming IPO
client.calendar_ipo()

# Get recent and coming ICO
client.calendar_ico()