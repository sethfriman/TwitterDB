import datetime
import sys
import time
import random
import pandas as pd
import psycopg2
from tqdm import tqdm

from RDBinteract import RDBInteract
from Tweet import Tweet
from dbConnect import DBConnect

if __name__ == '__main__':

    # Change variable to 'test' to run with test files, or 'full' to run with the full dataset
    run = 'full'
    if run == 'full':
        tweets = pd.read_csv('tweet.csv')
        follows = pd.read_csv('follows.csv')
    else:
        tweets = pd.read_csv('tweets_sample.csv')
        follows = pd.read_csv('follows_sample.csv')

    # Replace path with own path
    authentication_path = "/Users/sethfriman/Documents/authentication/twitterDBPostgres.txt"
    f = open(authentication_path, "r")
    lines = f.readlines()
    username = lines[0]
    password = lines[1]
    f.close()

    # Connect to the Postgres database TwitterDB
    try:
        connection = DBConnect('TwitterDB', username, password)
        interaction = RDBInteract()
        print('Connected to Database')
    except psycopg2.OperationalError as e:
        print('Could Not Connect to Database. Program Quitting')
        sys.exit(0)

    # Start the DB fresh
    connection.cursor.execute("SELECT count(*) FROM \"Tweet\"")
    print('Tweet table size before: ', connection.cursor.fetchall()[0][0])
    print('Clearing DB')
    connection.cursor.execute("Truncate \"Tweet\"")
    connection.cursor.execute("Truncate \"Follows\"")
    connection.cursor.execute("SELECT count(*) FROM \"Tweet\"")
    print('Tweet table size after: ', connection.cursor.fetchall()[0][0])

    # Adds the followers to the database
    fol_start_time = time.time()
    print('-----------------ADDING FOLLOWERS----------------')
    for index, row in tqdm(follows.iterrows()):
        interaction.insert_follow(row['USER_ID'], row['FOLLOWS_ID'], connection.cursor)
    print('FOLLOW ADD TIME: ' + str(round((time.time() - fol_start_time) / 60, 3)) + ' min')

    # Adds the tweets to the database
    tweet_start_time = time.time()
    print('-----------------ADDING TWEETS----------------')
    for index, row in tqdm(tweets.iterrows()):
        temp_tweet = Tweet(row['USER_ID'], index, time.time(), row['TWEET_TEXT'])
        interaction.insert_tweet(temp_tweet, connection.cursor)
    tweet_end_time = time.time()
    print('TWEET ADD TIME: ' + str(round((tweet_end_time - tweet_start_time) / 60, 3)) + ' min')
    print('Tweets per second: ', round(len(tweets) / (tweet_end_time - tweet_start_time), 3))

    connection.conn.commit()

    # Randomly selects 500 users and returns their timelines
    all_users = interaction.get_unique_users(connection.cursor)
    timeline_time = time.time()
    iters = 500
    print('-----------------Retrieving ' + str(iters) + ' Timelines---------------')
    for i in tqdm(range(500)):
        test_user = random.choice(all_users)
        user_timeline = interaction.get_timeline(test_user, connection.cursor)
    timeline_end_time = time.time()
    print('Time for 500 timelines: ' + str(round((timeline_end_time - timeline_time) / 60, 3)) + ' min')
    print('Timelines per second: ', round(iters / (timeline_end_time - timeline_time), 3))

    # Prints a random user's timeline to show string outputs
    test_user = random.choice(all_users)
    user_timeline = interaction.get_timeline(test_user, connection.cursor)
    print('----------------SAMPLE TIMELINE: USER ' + str(test_user) + '------------------')
    for tweet in user_timeline:
        print(str(tweet))

    connection.conn.close()
