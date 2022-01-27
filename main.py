import sys

import pandas as pd
import datetime

from RDBinteract import RDBInteract
from dbConnect import DBConnect

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    tweets = pd.read_csv('tweets_sample.csv')
    follows = pd.read_csv('follows_sample.csv')

    connection = DBConnect('TwitterDB', 'postgres', 'password')

    ## Replace with code to add
    for index, row in follows.iterrows():
        connection.cursor.execute("INSERT INTO \"Follows\" (user_id,follows_id) \
                  VALUES (" + str(row['USER_ID']) + ", " + str(row['FOLLOWS_ID']) + ")")
    connection.cursor.execute("SELECT * From \"Follows\"")
    rows = connection.cursor.fetchall()
    for row in rows:
        print(row[0], row[1])

    for index, row in tweets.iterrows():
        connection.cursor.execute("INSERT INTO \"Tweet\" (tweet_id,user_id, tweet_text, tweet_ts) \
                  VALUES (" + str(index) + ", " + str(row['USER_ID']) + ", '" + row['TWEET_TEXT'] +
                                  "', CURRENT_TIMESTAMP" + ")")

    connection.cursor.execute("SELECT * From \"Tweet\"")
    rows = connection.cursor.fetchall()
    for row in rows:
        print(row[0], row[1], row[2], row[3])

    interaction = RDBInteract()
    user_2_timeline = interaction.get_timeLine(1, connection.cursor)

    print('USER 1 TIMELINE')
    for row in user_2_timeline:
        print(row[0], row[1], row[2], row[3])
