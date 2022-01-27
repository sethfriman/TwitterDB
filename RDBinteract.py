from TweetAPI import TweetAPI


class RDBInteract(TweetAPI):

    def insertTweet(self, tweet):
        self.cursor.execute("INSERT INTO \"Tweet\" (tweet_id,user_id,tweet_ts,tweet_text) \
                  VALUES (tweet.get_tweet_id(), tweet.get_user_id(), tweet.get_ts(), tweet.get_text())")

    def getTimeline(self, user_id):
        self.cursor.execute("SELECT follows_id from \"Follows\" f where user_id = " + user_id)
        follows_ids = set(self.cursor.fetchall() + [user_id])
        print(follows_ids)
        self.cursor.execute("SELECT * from \"Tweet\" where user_id in " + follows_ids + " sort by tweet_ts desc")