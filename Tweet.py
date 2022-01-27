class Tweet :

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

        




