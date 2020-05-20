import json
import praw
import requests
from prawcore import NotFound
from .sentiment import SentimentMessage


class RedditSentiment(object):
    """Reddit lexical analysis about a financial value"""

    def __init__(self, config=None):
        self.scores = []
        self.parsed_urls = []
        self.config = None
        self.reddit = None
        if config:
            self.config = config
            self.reddit = praw.Reddit(**self.config)

    def _exists_on_reddit(self, term: str):
        exists = True
        try:
            self.reddit.subreddits.search_by_name(term, exact=True)
        except NotFound:
            exists = False
        return exists

    def get_reddit_post(self, term: str):
        reddit = praw.Reddit(**self.config)
        if reddit:
            posts = reddit.subreddit(term).hot(limit=100)
            return [SentimentMessage(post.selftext) for post in posts if len(post.selftext) > 0]

    def _get_sentiment_from_wayback(self, term: str, date):
        date_str = f'{date.year}{date.month:02d}{date.day:02d}'
        reddit_request = requests.get(f'http://archive.org/wayback/available?url=reddit.com/r/{term}&timestamp={date_str}')
        if reddit_request.status_code == 200:
            data1 = json.loads(reddit_request.text)
            archive_url = data1['archived_snapshots']['closest']['url']
            if archive_url not in self.parsed_urls:
                self.parsed_urls.append(archive_url)
        else:
            return -1
        # Should parse content of all pages
        return 1
