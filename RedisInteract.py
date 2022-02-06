import redis
from TweetAPI import TweetAPI


class RedisInteract(TweetAPI):

    def __init__(self):
        self.r = redis.Redis(
            host='localhost',
            port=6379
        )

    def insert_tweet(self, tweet):
        key_value = "tweet_" + str(tweet.tweet_id)
        self.r.hmset(key_value, {"tweet_id": tweet.tweet_id, "user_id": tweet.user_id,
                                 "timestamp": tweet.ts, "text": tweet.text})
        self.r.lpush("tweets", key_value)
        pass

    def insert_follow(self, user_id, follow_id):
        """inserts a follow to the database"""
        pass

    def get_timeline(self, user_id):
        """returns the timeline of the specified user"""
        pass

    def get_unique_users(self):
        """returns the unique users that have a follower or follow someone"""
        pass

    def clear_tables(self):
        """clears the tables in the database"""
        self.r.flushdb()

    def get_table_size(self, tablename):
        """returns the size of the specified table"""
        return self.r.llen(tablename)

    def commit(self):
        """commits all changes to the database"""
        pass

    def close(self):
        """closes the connection to the database"""
        self.r.close()
