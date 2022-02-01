from Tweet import Tweet
from TweetAPI import TweetAPI


class RDBInteract(TweetAPI):

    def insert_tweet(self, tweet, cursor):
        cursor.execute("INSERT INTO \"Tweet\" (tweet_id,user_id,tweet_ts,tweet_text) \
                  VALUES (" + str(tweet.get_tweet_id()) + ", " + str(tweet.get_user_id()) + ", to_timestamp(" +
                       str(tweet.get_ts()) + "), '" + tweet.get_text() + "')")

    def insert_follow(self, user_id, follow_id, cursor):
        cursor.execute("INSERT INTO \"Follows\" (user_id,follows_id) \
                          VALUES (" + str(user_id) + ", " + str(follow_id) + ")")

    def get_timeline(self, user_id, cursor):
        cursor.execute("SELECT * from \"Tweet\" where user_id in (SELECT follows_id from \"Follows\" f where user_id = "
                       + str(user_id) + ") order by tweet_ts desc limit 10")
        user_timeline = cursor.fetchall()
        user_timeline = [Tweet(row[0], row[2], row[1], row[2]) for row in user_timeline]
        return user_timeline

    def get_unique_users(self, cursor):
        cursor.execute("SELECT distinct follows_id as user_id from \"Follows\" union "
                       "SELECT distinct user_id as user_id from \"Follows\"")
        follow_ids = [item[0] for item in cursor.fetchall()]
        return follow_ids

