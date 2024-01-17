import numpy as np
import pandas as pd
import sqlite3

# Create db instance, connection and cursor to db instance
name = "bulking.db"
con = sqlite3.connect(":memory:")
cur = con.cursor()

# Functions to insert data
def insert_data(data):
    '''
    ---
    data = tuple
    if inserting multiple rows uses executemany
    '''
    if len(data) > 1:
        cur.execute("INSERT INTO bulking VALUES(?)",data)
    else:
        cur.executemany(
            "INSERT INTO data VALUES(?,?,?)",data)

def collect_user_data():
    current_weight = float(input("Enter your current weight: "))
    date = input("Enter the date in MMMDDYYYY format: ")
    workout_days = int(input("Enter the number of workout days this week: "))
    missed_meals = int(input("Enter the number of missed meals this week: "))
    protein_intake = int(input("Did you consume protein? (0 for No, 1 for Yes): "))
    creatine_intake = int(input("Did you consume creatine? (0 for No, 1 for Yes): "))
    
    data_tuple = (current_weight, date, workout_days, missed_meals, protein_intake, creatine_intake)
    return data_tuple

def run_collect_user_data():
    user_data = collect_user_data()
    return user_data

try:
    # Check if database is already created
    res = cur.execute("SELECT name FROM sqlite_master WHERE name='bulking'")
    table_exists = res.fetchone()
    if table_exists is None:
        print('Bulking does not exist, creating Bulking.')
        cur.execute("CREATE TABLE bulking(date, weight, workout_days, missed_meals, protein, creatine)")
        data_tuple = run_collect_user_data()
        print("Entering data into database..")
        cur.execute("INSERT INTO bulking VALUES (?,?,?,?,?,?)", data_tuple)
        
    else: # Insert data into db
        print("Bulking already exists, inserting data into table.")
        data_tuple = run_collect_user_data()
        cur.execute("INSERT INTO bulking VALUES (?,?,?,?,?,?)", data_tuple)

    # Commit changes to the db
    print("Committing changes...")
    con.commit()

except sqlite3.Error as e:
    print("SQLite error:", e)
    print('There was an issue with the database operation.')

finally:
    # Check for recent insert
    print("Most recent inserts, descending:")
    for row in con.execute("SELECT date, weight FROM bulking ORDER BY date"):
        print(row)
    # Close connection to db
    print("Closing the connection to Bulking..")
    con.close()






