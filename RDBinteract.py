import psycopg2

from Tweet import Tweet
from TweetAPI import TweetAPI


class RDBInteract(TweetAPI):
    """Class for interacting with the relational database. Contains methods for common Twitter interactions"""

    def __init__(self, dbname, dbuser, dbpassword):
        self.conn = psycopg2.connect("dbname=" + dbname + " user=" + dbuser + " password=" + dbpassword)
        self.cursor = self.conn.cursor()
        self.next_index = -1

    def insert_tweet(self, tweet):
        """inserts a tweet to the database"""
        self.cursor.execute("INSERT INTO \"Tweet\" (tweet_id,user_id,tweet_ts,tweet_text) \
                  VALUES (" + str(tweet.get_tweet_id()) + ", " + str(tweet.get_user_id()) + ", to_timestamp(" +
                       str(tweet.get_ts()) + "), '" + tweet.get_text() + "')")

    def insert_follow(self, user_id, follow_id):
        """inserts a follower into the database"""
        self.cursor.execute("INSERT INTO \"Follows\" (user_id,follows_id) \
                          VALUES (" + str(user_id) + ", " + str(follow_id) + ")")

    def get_timeline(self, user_id):
        """returns the timeline of the specified user. Timeline is 10 most recent tweets"""
        self.cursor.execute("SELECT * from \"Tweet\" t join \"Follows\" f on t.user_id = f.follows_id where f.user_id = "
                       + str(user_id) + " order by t.tweet_ts desc limit 10")
        user_timeline = self.cursor.fetchall()
        user_timeline = [Tweet(row[0], row[2], row[1], row[3]) for row in user_timeline]
        return user_timeline

    def get_unique_users(self):
        """returns the list of unique users from the database"""
        self.cursor.execute("SELECT distinct follows_id as user_id from \"Follows\" union "
                       "SELECT distinct user_id as user_id from \"Follows\"")
        follow_ids = [item[0] for item in self.cursor.fetchall()]
        return follow_ids

    def clear_tables(self):
        """clears the tables in the database"""
        self.cursor.execute("Truncate \"Tweet\"")
        self.cursor.execute("Truncate \"Follows\"")

    def get_table_size(self, tablename):
        """returns the size of the specified table"""
        self.cursor.execute("SELECT count(*) FROM \"" + tablename + "\"")
        return self.cursor.fetchall()[0][0]

    def get_next_index(self):
        """closes the connection to the database"""
        self.next_index += 1
        return self.next_index

    def commit(self):
        """commits all changes to the database"""
        self.conn.commit()

    def close(self):
        """closes the connection to the database"""
        self.conn.close()
