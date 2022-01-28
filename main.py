import datetime
import time
import random
import pandas as pd
from tqdm import tqdm

from RDBinteract import RDBInteract
from Tweet import Tweet
from dbConnect import DBConnect

if __name__ == '__main__':
    tweets = pd.read_csv('tweet.csv')
    follows = pd.read_csv('follows.csv')

    connection = DBConnect('TwitterDB', 'postgres', 'password')
    interaction = RDBInteract()

    fol_start_time = time.time()
    print('-----------------ADDING FOLLOWERS----------------')
    for index, row in tqdm(follows.iterrows()):
        interaction.insert_follow(row['USER_ID'], row['FOLLOWS_ID'], connection.cursor)
    print('FOLLOW ADD TIME: ' + str(round((time.time() - fol_start_time) / 60, 3)) + ' min')

    tweet_start_time = time.time()
    print('-----------------ADDING TWEETS----------------')
    for index, row in tqdm(tweets.iterrows()):
        temp_tweet = Tweet(row['USER_ID'], index, time.time(), row['TWEET_TEXT'])
        interaction.insert_tweet(temp_tweet, connection.cursor)
    tweet_end_time = time.time()
    print('TWEET ADD TIME: ' + str(round((tweet_end_time - tweet_start_time) / 60, 3)) + ' min')
    print('Tweets per second: ', round(1000000 / (tweet_end_time - tweet_start_time), 3))

    all_users = interaction.get_unique_users(connection.cursor)
    timeline_time = time.time()
    print('-----------------Retrieving 500 Timelines---------------')
    for i in tqdm(range(500)):
        test_user = random.choice(all_users)
        user_timeline = interaction.get_timeline(test_user, connection.cursor)
    timeline_end_time = time.time()
    print('Time for 500 timelines: ' + str(round((timeline_end_time - timeline_time) / 60, 3)) + ' min')
    print('Timelines per second: ', round(500 / (timeline_end_time - timeline_time), 3))

    test_user = random.choice(all_users)
    user_timeline = interaction.get_timeline(test_user, connection.cursor)
    print('----------------USER ' + str(test_user) + ' TIMELINE----------------')
    for row in user_timeline:
        print('Tweet #: ' + str(row[2]) + ' | User: ' + str(row[0]) + ' | Time: ' + str(row[1]))
        print('\tTweet: ', row[3])
