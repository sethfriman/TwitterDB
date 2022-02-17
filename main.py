import argparse
import csv
import os

from Run import Run
from Interaction import Interaction

def setTweets(type_of_run):
    """sets the tweets for the run based on run type (full or test) and extracts tweets from appropriate csv"""
    if type_of_run == 'full':
        tweet_file = open('tweet.csv')
    else:
        tweet_file = open('tweets_sample.csv')
    tweets = csv.reader(tweet_file)
    next(tweets)  # skip headers
    return tweets


def setFollows(type_of_run):
    """sets the followers for the run based on run type (full or test) and extracts followers from appropriate csv"""
    if type_of_run == 'full':
        follow_file = open('follows.csv')
    else:
        follow_file = open('follows_sample.csv')
    follows = csv.reader(follow_file)
    next(follows)
    return follows


if __name__ == '__main__':
    """
        Main program. Connects to DB, loads all Tweets and Follow combinations, and returns timelines.
        Documents run times for each process.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--db', default='redis', help='the database to use')
    parser.add_argument('-r', '--run', default='test', help='test or full. type of run')
    parser.add_argument('-s', '--strat', default='1', help='1 or 2, the strategy to employ for tweet insertions')
    args = parser.parse_args()

    #sets basic run information for database interaction based on database type, run type, and strategy  
    run = Run(args.db, args.run, args.strat)
    run.printRunInfo()
    tweets = setTweets(run.type_of_run)
    follows = setFollows(run.type_of_run)

    # Source username and password from environment variables
    username = os.environ.get('twitterdb_username')
    password = os.environ.get('twitterdb_password')

    #creates an interaction object that allows interaction with chosen database
    interact = Interaction(run.db, username, password)
   
    #resets the database from previous runs 
    interact.resetDB()
    #adds followers to the database
    interact.addFollowers(follows)
    #adds tweets to the database
    interact.addTweets(tweets, run)
 
    # Once everything is added, program commits insertions to the DB
    interact.commit()

    # Randomly selects 500 users and returns their timelines
    all_users = interact.get_unique_users()
    iters = 2000
    final_user, user_timeline = interact.getTimelines(iters, all_users, run)

    # Prints the last selected user's timeline to show string outputs
    print('----------------SAMPLE TIMELINE: USER ' + str(final_user) + '------------------')
    for tweet in user_timeline:
        print(str(tweet))

    # Close the connection to the DB
    interact.close()

