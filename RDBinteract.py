from Tweet import Tweet
from TweetAPI import TweetAPI


class RDBInteract(TweetAPI):
    """Class for interacting with the relational database. Contains methods for common Twitter interactions"""

    def insert_tweet(self, tweet, cursor):
        """inserts a tweet to the database"""
        cursor.execute("INSERT INTO \"Tweet\" (tweet_id,user_id,tweet_ts,tweet_text) \
                  VALUES (" + str(tweet.get_tweet_id()) + ", " + str(tweet.get_user_id()) + ", to_timestamp(" +
                       str(tweet.get_ts()) + "), '" + tweet.get_text() + "')")

    def insert_follow(self, user_id, follow_id, cursor):
        """inserts a follower into the database"""
        cursor.execute("INSERT INTO \"Follows\" (user_id,follows_id) \
                          VALUES (" + str(user_id) + ", " + str(follow_id) + ")")

    def get_timeline(self, user_id, cursor):
        """returns the timeline of the specified user. Timeline is 10 most recent tweets"""
        cursor.execute("SELECT * from \"Tweet\" t join \"Follows\" f on t.user_id = f.follows_id where f.user_id = "
                       + str(user_id) + " order by t.tweet_ts desc limit 10")
        user_timeline = cursor.fetchall()
        user_timeline = [Tweet(row[0], row[2], row[1], row[3]) for row in user_timeline]
        return user_timeline

    def get_unique_users(self, cursor):
        """returns the list of unique users from the database"""
        cursor.execute("SELECT distinct follows_id as user_id from \"Follows\" union "
                       "SELECT distinct user_id as user_id from \"Follows\"")
        follow_ids = [item[0] for item in cursor.fetchall()]
        return follow_ids

