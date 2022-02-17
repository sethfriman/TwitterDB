import datetime
import redis

from Tweet import Tweet
from TweetAPI import TweetAPI


class RedisInteract(TweetAPI):

    def __init__(self):
        self.r = redis.Redis(
            host='localhost',
            port=6379,
            decode_responses=True
        )
        self.r.set("next_index", -1)

    def insert_tweet(self, tweet):
        """Inserts a tweet into the database"""
        tweet.ts = datetime.datetime.utcfromtimestamp(tweet.ts).strftime('%Y-%m-%d %H:%M:%S.%f')
        key_value = "tweet_" + str(tweet.tweet_id)
        self.r.set(key_value, str(tweet.tweet_id) + "|" + str(tweet.user_id) +
                   "|" + str(tweet.ts) + "|" + tweet.text)
        self.r.lpush("user_" + str(tweet.user_id) + "_tweets", key_value)

    def insert_follow(self, user_id, follow_id):
        """inserts a follow to the database"""
        self.r.rpush("user_" + str(user_id) + "_follows", "user_" + str(follow_id))
        self.r.rpush("user_" + str(follow_id) + "_followedby", "user_" + str(user_id))
        self.r.sadd("users", str(user_id), str(follow_id))

    def get_timeline(self, user_id):
        """returns the timeline of the specified user"""
        following = self.get_following(user_id)
        following_ids = [followee[followee.index("_") + 1:] for followee in following]
        tweets_list = []
        for followee in following_ids:
            tweets = self.r.lrange("user_" + str(followee) + "_tweets", 0, 9)
            for tweet in tweets:
                temp_tweet = self.r.get(tweet)
                temp_tweet = temp_tweet.split("|")
                t_tweet_object = Tweet(int(temp_tweet[1]),
                                       int(temp_tweet[0]),
                                       temp_tweet[2],
                                       temp_tweet[3])
                tweets_list.append(t_tweet_object)
        tweets_list = sorted(tweets_list)
        return tweets_list[:10]

    def get_followers(self, user_id):
        """Returns the users that follow this user"""
        return self.r.lrange("user_" + str(user_id) + "_followedby", 0, -1)

    def get_following(self, user_id):
        """Returns the users that this user follows"""
        return self.r.lrange("user_" + str(user_id) + "_follows", 0, -1)

    def get_unique_users(self):
        """returns the unique users that have a follower or follow someone"""
        users = self.r.smembers("users")
        return [int(user) for user in users]

    def clear_tables(self):
        """clears the tables in the database"""
        self.r.flushdb()

    def get_table_size(self, tablename):
        """returns the size of the specified table"""
        return self.r.llen(tablename)

    def get_next_index(self):
        """returns the next available ID for a tweet"""
        return self.r.incr("next_index")

    def commit(self):
        """commits all changes to the database"""
        pass

    def close(self):
        """closes the connection to the database"""
        self.r.close()

    def make_tweet(self, tweet_id):
        temp_tweet = self.r.get(tweet_id)
        temp_tweet = temp_tweet.split("|")
        t_tweet_object = Tweet(int(temp_tweet[1]),
                               int(temp_tweet[0]),
                               temp_tweet[2],
                               temp_tweet[3])
        return t_tweet_object

    def insert_tweet_strat2(self, tweet):
        """Inserts a tweet into the database using a timeline-first strategy"""
        tweet.ts = datetime.datetime.utcfromtimestamp(tweet.ts).strftime('%Y-%m-%d %H:%M:%S.%f')
        key_value = "tweet_" + str(tweet.tweet_id)
        self.r.set(key_value, str(tweet.tweet_id) + "|" + str(tweet.user_id) +
                   "|" + str(tweet.ts) + "|" + tweet.text)
        followers = self.get_followers(tweet.user_id)
        for follower in followers:
            self.r.lpush(follower + "_timeline", key_value)

    def get_timeline_strat2(self, user_id):
        """returns the timeline of the specified user using the timeline-first strategy"""
        timeline_keys = self.r.lrange("user_" + str(user_id) + "_timeline", 0, 9)
        timeline_tweets = [self.make_tweet(tweet_id) for tweet_id in timeline_keys]
        return timeline_tweets
