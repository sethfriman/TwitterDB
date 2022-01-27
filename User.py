class User:

    def __init__(self, user_id, followers = [], following = [], tweets = []):
        self.followers = followers
        self.following = following
        self.tweets = tweets
        self.user_id = user_id

    def get_followers(self):
        return self.followers

    def get_following(self):
        return self.following

    def get_tweets(self):
        return self.tweets

    def get_user_id(self):
        return self.user_id