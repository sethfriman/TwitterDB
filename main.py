import argparse
import csv
import os
import sys
import time
import random
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

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--db', default='redis', help='the database to use')
    parser.add_argument('-r', '--run', default='full', help='test or full. type of run')
    parser.add_argument('-s', '--strat', default='1', help='1 or 2, the strategy to employ for tweet insertions')
    args = parser.parse_args()

    if (args.strat == "1") | (args.db != "redis"):
        strat = "1"
    elif args.strat == "2":
        strat = "2"
    else:
        print("Note - Strategy: \'" + str(args.strat) + "\' does not exist. Running Strategy 1")
        strat = "1"

    print("--------Run Info---------")
    print('Database: ', args.db)
    print('Type: ', args.run)
    print('Strategy: ', strat)
    print()

    if args.run == 'full':
        tweet_file = open('tweet.csv')
        follow_file = open('follows.csv')
        tweets = csv.reader(tweet_file)
        follows = csv.reader(follow_file)
        next(tweets)  # skip headers
        next(follows)
    else:
        tweet_file = open('tweets_sample.csv')
        follow_file = open('follows_sample.csv')
        tweets = csv.reader(tweet_file)
        follows = csv.reader(follow_file)
        next(tweets)  # skip headers
        next(follows)

    # Source username and password from environment variables
    username = os.environ.get('twitterdb_username')
    password = os.environ.get('twitterdb_password')

    # Connect to the specified database TwitterDB
    if args.db == 'postgres':
        try:
            interaction = RDBInteract('TwitterDB', username, password)
            print('Connected to Database')
        except psycopg2.OperationalError as e:
            print('Could Not Connect to Database. Program Quitting')
            sys.exit(0)
    elif args.db == 'redis':
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
    print('Num user found before: ', len(interaction.get_unique_users()))
    print('Clearing DB')
    interaction.clear_tables()
    print('Num user found after: ', len(interaction.get_unique_users()))

    # Adds the followers to the database
    fol_start_time = time.time()
    print('-----------------ADDING FOLLOWERS----------------')
    for row in follows:
        interaction.insert_follow(row[0], row[1])
    print('FOLLOW ADD TIME: ' + str(round((time.time() - fol_start_time) / 60, 3)) + ' min')

    # Adds the tweets to the database
    tweet_start_time = time.time()
    print('-----------------ADDING TWEETS----------------')
    index = 0
    for row in tweets:
        if index % 10000 == 0:
            if index != 0:
                print("\033[A                             \033[A")
            print('Current Index: ', index)
        temp_tweet = Tweet(row[0], interaction.get_next_index(), time.time(), row[1])
        if strat == "1":
            interaction.insert_tweet(temp_tweet)
        elif strat == "2":
            interaction.insert_tweet_strat2(temp_tweet)
        else:
            interaction.insert_tweet(temp_tweet)
        index += 1
    tweet_end_time = time.time()
    print("\033[A                             \033[A")
    print('TWEET ADD TIME: ' + str(round((tweet_end_time - tweet_start_time) / 60, 3)) + ' min')
    print('Tweets per second: ', round(index / (tweet_end_time - tweet_start_time), 3))

    # Once everything is added, program commits insertions to the DB
    interaction.commit()

    # Randomly selects 500 users and returns their timelines
    all_users = interaction.get_unique_users()
    timeline_time = time.time()
    iters = 500
    print('-----------------Retrieving ' + str(iters) + ' Timelines---------------')
    for i in range(iters):
        test_user = random.choice(all_users)
        if strat == "1":
            user_timeline = interaction.get_timeline(test_user)
        elif strat == "2":
            user_timeline = interaction.get_timeline_strat2(test_user)
        else:
            user_timeline = interaction.get_timeline(test_user)
    timeline_end_time = time.time()
    print('Time for ' + str(iters) + ' timelines: ' + str(round((timeline_end_time - timeline_time) / 60, 3)) + ' min')
    print('Timelines per second: ', round(iters / (timeline_end_time - timeline_time), 3))

    # Prints the last selected user's timeline to show string outputs
    print('----------------SAMPLE TIMELINE: USER ' + str(test_user) + '------------------')
    for tweet in user_timeline:
        print(str(tweet))

    # Close the connection to the DB
    interaction.close()
