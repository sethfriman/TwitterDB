import os
import sys
import time
import random
import pandas as pd
import psycopg2
import redis.exceptions

from RDBinteract import RDBInteract
from RedisInteract import RedisInteract
from Tweet import Tweet
from dbConnect import DBConnect

if __name__ == '__main__':
    """
        Main program. Connects to DB, loads all Tweets and Follow combinations, and returns timelines.
        Documents run times for each process.
    """

    # Change variable to 'test' to run with test files, or 'full' to run with the full dataset
    run = 'full'
    if run == 'full':
        tweets = pd.read_csv('tweet.csv')
        follows = pd.read_csv('follows.csv')
    else:
        tweets = pd.read_csv('tweets_sample.csv')
        follows = pd.read_csv('follows_sample.csv')

    # Source username and password from environment variables
    username = os.environ.get('twitterdb_username')
    password = os.environ.get('twitterdb_password')

    # Connect to the specified database TwitterDB
    db = 'postgres'
    if db == 'postgres':
        try:
            interaction = RDBInteract('TwitterDB', username, password)
            print('Connected to Database')
        except psycopg2.OperationalError as e:
            print('Could Not Connect to Database. Program Quitting')
            sys.exit(0)
    elif db == 'redis':
        try:
            interaction = RedisInteract()
            print('Connected to Database')
        except redis.exceptions.ConnectionError as e:
            print('Could Not Connect to Database. Program Quitting')
            sys.exit(0)
    else:
        print('Specified Database is not supported. Program Quitting')
        sys.exit(0)

    # Start the DB fresh
    interaction.clear_tables()
    print('Tweet table size before: ', interaction.get_table_size("Tweet"))
    print('Clearing DB')
    interaction.clear_tables()
    print('Tweet table size after: ', interaction.get_table_size("Tweet"))

    # Adds the followers to the database
    fol_start_time = time.time()
    print('-----------------ADDING FOLLOWERS----------------')
    for index, row in follows.iterrows():
        interaction.insert_follow(row['USER_ID'], row['FOLLOWS_ID'])
    print('FOLLOW ADD TIME: ' + str(round((time.time() - fol_start_time) / 60, 3)) + ' min')

    # Adds the tweets to the database
    tweet_start_time = time.time()
    print('-----------------ADDING TWEETS----------------')
    for index, row in tweets.iterrows():
        if index % 10000 == 0:
            if index != 0:
                print("\033[A                             \033[A")
            print('Current Index: ', index)
        temp_tweet = Tweet(row['USER_ID'], index, time.time(), row['TWEET_TEXT'])
        interaction.insert_tweet(temp_tweet)
    tweet_end_time = time.time()
    print("\033[A                             \033[A")
    print('TWEET ADD TIME: ' + str(round((tweet_end_time - tweet_start_time) / 60, 3)) + ' min')
    print('Tweets per second: ', round(len(tweets) / (tweet_end_time - tweet_start_time), 3))

    # Once everything is added, program commits insertions to the DB
    interaction.commit()

    # Randomly selects 500 users and returns their timelines
    all_users = interaction.get_unique_users()
    timeline_time = time.time()
    iters = 500
    print('-----------------Retrieving ' + str(iters) + ' Timelines---------------')
    for i in range(iters):
        test_user = random.choice(all_users)
        user_timeline = interaction.get_timeline(test_user)
    timeline_end_time = time.time()
    print('Time for ' + str(iters) + ' timelines: ' + str(round((timeline_end_time - timeline_time) / 60, 3)) + ' min')
    print('Timelines per second: ', round(iters / (timeline_end_time - timeline_time), 3))

    # Prints a random user's timeline to show string outputs
    test_user = random.choice(all_users)
    user_timeline = interaction.get_timeline(test_user)
    print('----------------SAMPLE TIMELINE: USER ' + str(test_user) + '------------------')
    for tweet in user_timeline:
        print(str(tweet))

    # Close the connection to the DB
    interaction.close()
