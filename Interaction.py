import sys
import time
import redis
from RedisInteract import RedisInteract
from Tweet import Tweet

class Interaction:
    """Object that represents an Interaction and holds the relevant metadata"""

    def __init__(self, db):
        self.db = db
        self.specific_interact = self.connect()
        
    def connect(self):
        # Connect to the specified database TwitterDB
        if self.db == 'postgres':
                    # try:
                #   interaction = RDBInteract('TwitterDB', username, password)
                #   print('Connected to Database')
            #  except psycopg2.OperationalError as e:
            #     print('Could Not Connect to Database. Program Quitting')
            #     sys.exit(0)
            print()
        elif self.db == 'redis':
            try:
                interaction = RedisInteract()
                print('Connected to Database')
                return interaction
            except redis.exceptions.ConnectionError as e:
                print('Could Not Connect to Database. Program Quitting')
                sys.exit(0)
        else:
            print('Specified Database is not supported. Program Quitting')
            sys.exit(0)


    def resetDB(self):
        # Start the DB fresh
        print('Num user found before: ', len(self.specific_interact.get_unique_users()))
        print('Clearing DB')
        self.specific_interact.clear_tables()
        print('Num user found after: ', len(self.specific_interact.get_unique_users()))

    def addFollowers(self, follows):
        # Adds the followers to the database
        fol_start_time = time.time()
        print('-----------------ADDING FOLLOWERS----------------')
        for row in follows:
            self.specific_interact.insert_follow(row[0], row[1])
        print('FOLLOW ADD TIME: ' + str(round((time.time() - fol_start_time) / 60, 3)) + ' min')

    def addTweets(self, tweets, run):
        # Adds the tweets to the database
        tweet_start_time = time.time()
        print('-----------------ADDING TWEETS----------------')
        index = 0
        for row in tweets:
            if index % 10000 == 0:
                if index != 0:
                    print("\033[A                             \033[A")
                print('Current Index: ', index)
            temp_tweet = Tweet(row[0], self.specific_interact.get_next_index(), time.time(), row[1])
            if run.strat == "1":
                self.specific_interact.insert_tweet(temp_tweet)
            elif run.strat == "2":
                self.specific_interact.insert_tweet_strat2(temp_tweet)
            else:
                self.specific_interact.insert_tweet(temp_tweet)
            index += 1
        tweet_end_time = time.time()
        print("\033[A                             \033[A")
        print('TWEET ADD TIME: ' + str(round((tweet_end_time - tweet_start_time) / 60, 3)) + ' min')
        print('Tweets per second: ', round(index / (tweet_end_time - tweet_start_time), 3))
