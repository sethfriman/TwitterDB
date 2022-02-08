import abc


class TweetAPI(abc.ABC):
    """Interface for conducting interactions with a database"""

    @abc.abstractmethod
    def insert_tweet(self, tweet):
        """inserts a tweet to the database"""
        pass

    @abc.abstractmethod
    def insert_follow(self, user_id, follow_id):
        """inserts a follow to the database"""
        pass

    @abc.abstractmethod
    def get_timeline(self, user_id):
        """returns the timeline of the specified user"""
        pass

    @abc.abstractmethod
    def get_unique_users(self):
        """returns the unique users that have a follower or follow someone"""
        pass

    @abc.abstractmethod
    def clear_tables(self):
        """clears the tables in the database"""
        pass

    @abc.abstractmethod
    def get_table_size(self, tablename):
        """returns the size of the specified table"""
        pass

    @abc.abstractmethod
    def get_next_index(self):
        """returns the next available ID for a tweet"""
        pass

    @abc.abstractmethod
    def commit(self):
        """commits all changes to the database"""
        pass

    @abc.abstractmethod
    def close(self):
        """closes the connection to the database"""
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

