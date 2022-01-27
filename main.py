import sys
import time
import random

import pandas as pd
import datetime

from RDBinteract import RDBInteract
from dbConnect import DBConnect

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    tweets = pd.read_csv('tweet.csv')
    follows = pd.read_csv('follows.csv')

    connection = DBConnect('TwitterDB', 'postgres', 'password')

    ## Replace with code to add
    fol_start_time = time.time()
    print('-----------------ADDING FOLLOWERS----------------')
    for index, row in follows.iterrows():
        if index % 100000 == 0:
            print(index)
        connection.cursor.execute("INSERT INTO \"Follows\" (user_id,follows_id) \
                  VALUES (" + str(row['USER_ID']) + ", " + str(row['FOLLOWS_ID']) + ")")
    print('FOLLOW ADD TIME: ', (time.time() - fol_start_time) / 60, 'min')

    tweet_start_time = time.time()
    print('-----------------ADDING TWEETS----------------')
    for index, row in tweets.iterrows():
        if index % 100000 == 0:
            print(index)
        connection.cursor.execute("INSERT INTO \"Tweet\" (tweet_id,user_id, tweet_text, tweet_ts) \
                  VALUES (" + str(index) + ", " + str(row['USER_ID']) + ", '" + row['TWEET_TEXT'] +
                                  "', CURRENT_TIMESTAMP" + ")")
    print('TWEET ADD TIME: ', (time.time() - tweet_start_time) / 60, 'min')

    interaction = RDBInteract()

    test_user = random.choice(interaction.get_unique_users(connection.cursor))
    print(test_user)
    user_timeline = interaction.get_timeLine(test_user, connection.cursor)

    print('USER ' + str(test_user) + ' TIMELINE')
    for row in user_timeline:
        print(row[0], row[1], row[2], row[3])
