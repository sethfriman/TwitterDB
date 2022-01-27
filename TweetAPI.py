import abc

class TweetAPI(abc.ABC):

    @abc.abstractmethod
    def insert_tweet(self, tweet):
        pass

    @abc.abstractmethod
    def get_timeLine(self, User):
        pass

    @abc.abstractmethod
    def get_followers(self, User):
        pass
    
    @abc.abstractmethod
    def get_followees(self, User):
        pass

    @abc.abstractmethod
    def get_Tweets(self, User):
        pass
    

