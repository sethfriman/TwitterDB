from dbConnect import DBConnect

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connection = DBConnect('TwitterDB', 'postgres', 'password')

    ## Replace with code to add
    connection.cursor.execute("INSERT INTO \"Follows\" (user_id,follows_id) \
          VALUES (1, 2)")
    connection.cursor.execute("SELECT * From \"Follows\"")
    rows = connection.cursor.fetchall()
    for row in rows:
        print(row[0], row[1])
