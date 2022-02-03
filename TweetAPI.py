import abc


class TweetAPI(abc.ABC):
    """Interface for conducting interactions with a database"""

    @abc.abstractmethod
    def insert_tweet(self, tweet, cursor):
        """inserts a tweet to the database"""
        pass

    @abc.abstractmethod
    def get_timeline(self, user_id, cursor):
        """returns the timeline of the specified user"""
        pass

    # @abc.abstractmethod
    # def get_followers(self, User):
    #     pass
    #
    # @abc.abstractmethod
    # def get_followees(self, User):
    #     pass
    #
    # @abc.abstractmethod
    # def get_Tweets(self, User):
    #     pass
    #

