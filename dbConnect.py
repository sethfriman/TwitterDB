import psycopg2

class DBConnect:

    def __init__(self, dbname, dbuser, dbpassword):
        self.conn = psycopg2.connect("dbname=" + dbname + " user=" + dbuser + " password=" + dbpassword)
        self.cursor = self.conn.cursor()

