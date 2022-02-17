
class Run:
    """Object representing a Run"""

    def __init__(self, db, type_of_run, strat):
        if db == 'postgres':
            if strat == "2":
                print('There is no strategy 2 for Postgres, defaulting to strategy 1')
            strat = "1"
        self.db = db
        self.type_of_run = type_of_run
        self.strat = strat

    def printRunInfo(self):
        print("--------Run Info---------")
        print('Database: ', self.db)
        print('Type: ', self.type_of_run)
        print('Strategy: ', self.strat)
        print()
