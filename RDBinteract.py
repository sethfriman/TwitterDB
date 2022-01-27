from TweetAPI import TweetAPI


class RDBInteract(TweetAPI):

    def insert_tweet(self, tweet, cursor):
        cursor.execute("INSERT INTO \"Tweet\" (tweet_id,user_id,tweet_ts,tweet_text) \
                  VALUES (tweet.get_tweet_id(), tweet.get_user_id(), tweet.get_ts(), tweet.get_text())")

    def get_timeLine(self, user_id, cursor):
        cursor.execute("SELECT follows_id from \"Follows\" f where user_id = " + str(user_id))
        follow_ids = [item[0] for item in cursor.fetchall()]
        follows_ids = str(follow_ids + [user_id]).replace('[', '(').replace(']', ')')
        cursor.execute("SELECT * from \"Tweet\" where user_id in " + str(follows_ids) + " order by tweet_ts desc")
        return cursor.fetchall()

    def get_unique_users(self, cursor):
        cursor.execute("SELECT distinct follows_id as user_id from \"Follows\" union "
                       "SELECT distinct user_id as user_id from \"Follows\"")
        follow_ids = [item[0] for item in cursor.fetchall()]
        return follow_ids

