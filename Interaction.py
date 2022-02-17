import random
import sys
import time

import psycopg2
import redis

from RDBinteract import RDBInteract
from RedisInteract import RedisInteract
from Tweet import Tweet


class Interaction:
    """Object that represents an Interaction and holds the relevant metadata"""

    def __init__(self, db, username, password):
        self.db = db
        self.specific_interact = self.connect(username, password)
        
    def connect(self, username, password):
        """Connect to specified database TwitterDB"""
        if self.db == 'postgres':
            try:
                interaction = RDBInteract('TwitterDB', username, password)
                print('Connected to Database')
            except psycopg2.OperationalError as e:
                print('Could Not Connect to Database. Program Quitting')
                sys.exit(0)
            print()
        elif self.db == 'redis':
            try:
                interaction = RedisInteract()
                print('Connected to Database')
            except redis.exceptions.ConnectionError as e:
                print('Could Not Connect to Database. Program Quitting')
                sys.exit(0)
        else:
            print('Specified Database is not supported. Program Quitting')
            sys.exit(0)
        return interaction

    def resetDB(self):
        """Start the DB fresh"""
        print('Num user found before: ', len(self.specific_interact.get_unique_users()))
        print('Clearing DB')
        self.specific_interact.clear_tables()
        print('Num user found after: ', len(self.specific_interact.get_unique_users()))

    def addFollowers(self, follows):
        """Adds the followers to the database"""
        fol_start_time = time.time()
        print('-----------------ADDING FOLLOWERS----------------')
        for row in follows:
            self.specific_interact.insert_follow(row[0], row[1])
        print('FOLLOW ADD TIME: ' + str(round((time.time() - fol_start_time) / 60, 3)) + ' min')

    def addTweets(self, tweets, run):
        """Adds the tweets to the database"""
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

    def getTimelines(self, iters, all_users, run):
        """retrieves timelinens from database"""
        timeline_time = time.time()
        print('-----------------Retrieving ' + str(iters) + ' Timelines---------------')
        for i in range(iters):
            test_user = random.choice(all_users)
            if run.strat == "1":
                user_timeline = self.specific_interact.get_timeline(test_user)
            elif run.strat == "2":
                user_timeline = self.specific_interact.get_timeline_strat2(test_user)
            else:
                user_timeline = self.specific_interact.get_timeline(test_user)
        timeline_end_time = time.time()
        print('Time for ' + str(iters) + ' timelines: ' + str(
            round((timeline_end_time - timeline_time) / 60, 3)) + ' min')
        print('Timelines per second: ', round(iters / (timeline_end_time - timeline_time), 3))
        return test_user, user_timeline

    def commit(self):
        """ommits the interaction to database"""
        self.specific_interact.commit()

    def get_unique_users(self):
        """gets unique users from specific_interact"""
        return self.specific_interact.get_unique_users()

    def close(self):
        """closes the specific_interact interaction with database"""
        self.specific_interact.close()
