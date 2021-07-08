from newsapi.articles import Articles
from newsapi.sources import Sources

key='05897eee11194ab1861dff2adf064ac8' 

a = Articles(API_KEY=key)
a.get(source="bbc-news")['articles'][2]
s = Sources(API_KEY=key)
s.get(category='technology', language='en', country='uk')
s.get_by_country('us')