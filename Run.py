import argparse

class Run:
    """Object representing a Run"""

    def __init__(self, db, type_of_run, strat):
        self.db = db
        self.type_of_run = type_of_run
        self.strat = strat

    def setRun(db, test_or_full, strat):
        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--db', default=db, help='the database to use')
        parser.add_argument('-r', '--run', default=test_or_full, help='test or full. type of run')
        parser.add_argument('-s', '--strat', default=strat, help='1 or 2, the strategy to employ for tweet insertions')
        args = parser.parse_args()
        return args

    def printRunInfo(self):
        print("--------Run Info---------")
        print('Database: ', self.db)
        print('Type: ', self.type_of_run)
        print('Strategy: ', self.strat)
        print()
