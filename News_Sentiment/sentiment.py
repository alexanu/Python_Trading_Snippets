import re
import datetime
from textblob import TextBlob


class SentimentMessage(object):
    def __init__(self, text, created_at=None):
        self.text = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split())
        self.sentiment, self.polarity = SentimentMessage.calculate_sentiment(self.text)
        self.created_at = created_at if created_at is not None else datetime.datetime.now().strftime('%Y%m%d')

    def __str__(self):
        return f'{self.text} (Posted in {self.created_at})\nOverall : {self.sentiment} (polarity : {self.polarity})'

    def csv_dump(self):
        return f'{self.created_at},{self.polarity}\n'

    @staticmethod
    def calculate_sentiment(text: str) -> (str, int):
        analysis = TextBlob(text)
        sentiment = None
        if analysis.sentiment.polarity > 0:
            sentiment = 'positive'
        elif analysis.sentiment.polarity == 0:
            sentiment = 'neutral'
        else:
            sentiment = 'negative'
        return sentiment, analysis.sentiment.polarity
