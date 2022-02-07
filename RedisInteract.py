import datetime

import pandas as pd
import redis

from Tweet import Tweet
from TweetAPI import TweetAPI


class RedisInteract(TweetAPI):

    def __init__(self):
        self.r = redis.Redis(
            host='localhost',
            port=6379
        )

    def insert_tweet(self, tweet):
        """Inserts a tweet into the database"""
        tweet.ts = datetime.datetime.utcfromtimestamp(tweet.ts).strftime('%Y-%m-%d %H:%M:%S.%f')
        key_value = "tweet_" + str(tweet.tweet_id)
        self.r.hmset(key_value, {"tweet_id": tweet.tweet_id, "user_id": tweet.user_id,
                                 "timestamp": tweet.ts, "text": tweet.text})
        self.r.lpush("user_" + str(tweet.user_id) + "_tweets", key_value)

    def insert_follow(self, user_id, follow_id):
        """inserts a follow to the database"""
        self.r.rpush("user_" + str(user_id), "user_" + str(follow_id))
        self.r.sadd("users", str(user_id), str(follow_id))

    def get_timeline(self, user_id):
        """returns the timeline of the specified user"""
        following = self.r.lrange("user_" + str(user_id), 0, -1)
        following = [followee.decode('utf-8') for followee in following]
        following_ids = [followee[followee.index("_") + 1:] for followee in following]
        tweets_list = []
        tweet_df = pd.DataFrame(columns=['tweet_id', 'user_id', 'timestamp', 'text'])
        for followee in following_ids:
            tweets = self.r.lrange("user_" + str(followee) + "_tweets", 0, 10)
            for tweet in tweets:
                temp_tweet = self.r.hgetall(tweet)
                tweet_df = tweet_df.append({'tweet_id': temp_tweet[b'tweet_id'].decode("utf-8"),
                                            'user_id': temp_tweet[b'user_id'].decode("utf-8"),
                                            'timestamp': temp_tweet[b'timestamp'].decode("utf-8"),
                                            'text': temp_tweet[b'text'].decode("utf-8")}, ignore_index=True)
        tweet_df = tweet_df.sort_values('timestamp', ascending=False).reset_index(drop=True)
        tweet_df = tweet_df[:10]
        for i, row in tweet_df.iterrows():
            t_tweet_object = Tweet(row['user_id'], row['tweet_id'],
                                   row['timestamp'], row['text'])
            tweets_list.append(t_tweet_object)
        return tweets_list

    def get_unique_users(self):
        """returns the unique users that have a follower or follow someone"""
        users = self.r.smembers("users")
        return [int(user.decode("utf-8")) for user in users]

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
