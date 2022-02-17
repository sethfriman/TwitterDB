
import csv
import os
import sys
import time
import random
#import psycopg2
import redis.exceptions

from RDBinteract import RDBInteract
from RedisInteract import RedisInteract
from Tweet import Tweet
from dbConnect import DBConnect
from Run import Run
from Interaction import Interaction



def setTweets(type_of_run):
    if type_of_run == 'full':
        tweet_file = open('tweet.csv')
    else:
        tweet_file = open('tweets_sample.csv')
    tweets = csv.reader(tweet_file)
    next(tweets)  # skip headers
    return tweets


def setFollows(type_of_run):
    if type_of_run == 'full':
        follow_file = open('follows.csv')
    else:
        follow_file = open('follows_sample.csv')
    follows = csv.reader(follow_file)
    next(follows)
    return follows

#def connectPG():
    # try:
         #   interaction = RDBInteract('TwitterDB', username, password)
         #   print('Connected to Database')
      #  except psycopg2.OperationalError as e:
       #     print('Could Not Connect to Database. Program Quitting')
       #     sys.exit(0)
    #return interaction



if __name__ == '__main__':
    """
        Main program. Connects to DB, loads all Tweets and Follow combinations, and returns timelines.
        Documents run times for each process.
    """
    run = Run('redis', 'test', '1')
    Run.printRunInfo(run)
    tweets = setTweets(run.type_of_run)
    follows = setFollows(run.type_of_run)

    interact = Interaction(run.db)
   
    interact.resetDB()
    interaction = interact.connect()
    interact.addFollowers(follows)
    interact.addTweets(tweets, run)
    
    # Source username and password from environment variables
    username = os.environ.get('twitterdb_username')
    password = os.environ.get('twitterdb_password')
 
    # Once everything is added, program commits insertions to the DB
    interaction.commit()

    # Randomly selects 500 users and returns their timelines
    all_users = interaction.get_unique_users()
    timeline_time = time.time()
    iters = 2000
    print('-----------------Retrieving ' + str(iters) + ' Timelines---------------')
    for i in range(iters):
        test_user = random.choice(all_users)
        if run.strat == "1":
            user_timeline = interaction.get_timeline(test_user)
        elif run.strat == "2":
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

