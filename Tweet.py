import datetime
import time


class Tweet:
    """Object that represents a Tweet and holds the relevant metadata"""

    def __init__(self, user_id, tweet_id, ts, text):
        self.user_id = user_id
        self.tweet_id = tweet_id
        self.ts = ts
        self.text = text

    def get_tweet_id(self):
        return self.tweet_id

    def get_user_id(self):
        return self.user_id

    def get_ts(self):
        return self.ts

    def get_text(self):
        return self.text

    def __str__(self):
        return 'Tweet #: ' + str(self.tweet_id) + ' | User: ' + str(self.user_id) + ' | Time: ' + str(self.ts) + \
               '\n\tTweet: ' + str(self.text)

    def _cmp(self, other):
        """Returns the time difference between two tweets"""
        return time.mktime(datetime.datetime.strptime(other.ts, "%Y-%m-%d %H:%M:%S.%f").timetuple()) - \
               time.mktime(datetime.datetime.strptime(self.ts, "%Y-%m-%d %H:%M:%S.%f").timetuple())

    def __lt__(self, other):
        """A tweet is 'less than' another tweet if it was posted later"""
        return self._cmp(other) < 0
