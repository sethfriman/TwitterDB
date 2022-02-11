# TwitterDB
Pulls tweets from a sample file provided and uploads to a database based on the user's specifications

Then pulls timelines from a database based on who the selected user follows and the recency of their tweets

## Data Files
tweet.csv: file containing 1,000,000 tweets. Column headers are 'USER_ID' and 'TWEET_TEXT'
follows.csv: file containing roughly 30,000 rows of users that follow other users. Column headers are 'USER_ID' and 'FOLLOWS_ID' where the user specified in USER_ID follows the user specified in FOLLOWS_ID

## How to run:
To run the program, navigate to the project directory

The run command is 'python main.py' and takes two optional arguments
  '-d' or '--db': the database to use. Currently supports Redis ('redis') and Postgres ('postgres') (default is 'redis')
      Note: To use Redis db, Redis must be installed on the machine running the program. 
            To run Postgres db, Postgres must be installed and a database must already exist to hold the values in the follows.csv and tweet.csv files
            In both instances, the server for the database must already be running.
  '-r' or '--run': the type of run to do (either 'full' or 'test', default is 'full')
      Note: 'test' pulls the data from smaller test files to check for program completeness and observe bugs
            'full' pulls from a follows.csv file of roughly 30,000 follows combinations and tweet.csv file that holds 1,000,000 tweets

running just 'python main.py' will execute a complete run of the program. Current default arguments are 'full' (csv files) and 'redis' (database)

## What the Program Does
The program first attempts to connect to the specified database. If successful, it returns a success message and continues. An unsuccessful connection exits the program.

Using the specified database, the program will then insert all Follows combinations from the follows.csv file into the database. Returns the time in minutes to complete this process.

Next, tweets are pulled from the tweet.csv file one row at a time, converted to Tweet objects (user_id, tweet_id, timestamp, and text) using the next available tweet_id value and the current timestamp.
Returns the current index every 10,000 iterations (for progress) and then ultimately the total time for this process and the number of uploads executed per second on average.

The program then generates a list of unique users that either follow someone or have a follower. It selects a random user from this list and returns their timeline.
A timeline is the 10 most recent tweets from any user that the specified user follows.
This process is repeated 500 times.
Returns the total amount of time and the number of timelines the program/database were able to return per second.

Finally, The program prints a sample timeline from another random user to show sample output of a timeline, and closes the connection with the database, finishing the run.
