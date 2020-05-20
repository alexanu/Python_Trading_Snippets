import twython
import twint
from dateutil.parser import parse
from datetime import datetime
from .sentiment import SentimentMessage


class MyStreamer(twython.TwythonStreamer):
    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret):
        super().__init__(app_key, app_secret, oauth_token, oauth_token_secret)
        self.max_counts = 100
        self.tweets = []

    # Received data
    def on_success(self, data):
        # Only collect tweets in English
        if data['lang'] == 'en':
            self.tweets.append({
                "text": data["text"],
                "created_at": data["text"]
            })
        if len(self.tweets) == self.max_counts:
            self.disconnect()

    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()


class TwitterSentiment(object):
    """Twitter Sentiment about a topic or keyword"""
    def __init__(self, config):
        self.config = config
        self.twitter = twython.Twython(config['CONSUMER_KEY'], config['CONSUMER_SECRET'],
                                       config['ACCESS_TOKEN'], config['ACCESS_SECRET'])
        self.stream = MyStreamer(config['CONSUMER_KEY'], config['CONSUMER_SECRET'],
                                 config['ACCESS_TOKEN'], config['ACCESS_SECRET'])

    def get_tweets(self, query, since=None, until=None, count=100):
        c = twint.Config()
        if since is not None:
            c.Since = since
            if until is not None:
                c.Until = until
            else:
                c.Until = datetime.now().strftime("%Y-%m-%d")
        c.Search = query
        c.Limit = count
        c.Count = count
        c.Store_object = True
        twint.run.Search(c)
        tweets = twint.output.tweets_object
        return [SentimentMessage(tweet.tweet, datetime.strftime(datetime.fromtimestamp(tweet.datetime / 1e3), '%Y%m%d'))
                for tweet in tweets]

    def get_tweets_from_api(self, query, count):
        tweets = self.twitter.search(q=query, lang="en", count=count)
        print(len(tweets))
        tweets = tweets["statuses"]
        return [SentimentMessage(tweet["text"], datetime.strftime(parse(tweet["created_at"]), '%Y%m%d'))
                for tweet in tweets]

    def get_live_tweets(self, query, count):
        self.stream.max_count = count
        self.stream.statuses.filter(track=query)
        return [SentimentMessage(tweet["text"], datetime.strftime(tweet["created_at"], '%Y%m%d'))
                for tweet in self.stream.tweets]
