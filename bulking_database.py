import numpy as np
import pandas as pd
import sqlite3

# Create db instance, connection and cursor to db instance
name = "bulking"
con = sqlite3.connect(":memory:") # CHANGE TO "BULKING.DB" WHEN IT'S TIME TO DEPLOY TO PROD
cur = con.cursor()

# Functions to insert data
def insert_data(data):
    '''
    ---
    data = tuple
    If inserting multiple rows uses executemany
    '''
    len_of_data = len(data)
    marks = '?' * len_of_data


    if len(data) > 1:
        cur.execute(f"INSERT INTO {name} VALUES({marks})",data)
    else:
        cur.executemany(
            f"INSERT INTO data VALUES({marks})",data)

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
    res = cur.execute(f"SELECT name FROM sqlite_master WHERE name={name}")
    table_exists = res.fetchone()
    if table_exists is None:
        print('-' * 45)
        print('\n\tBulking does not exist, creating Bulking.\n')
        print('-' * 45)
        cur.execute(f"CREATE TABLE {name}(date, weight, workout_days, missed_meals, protein, creatine)")
        data_tuple = run_collect_user_data()
        print('-' * 45)
        print("\n\tEntering data into database..\n")
        print('-' * 45)
        cur.execute("INSERT INTO bulking VALUES (?,?,?,?,?,?)", data_tuple)
        
    else: # Insert data into db
        print("Bulking already exists, inserting data into table.")
        data_tuple = run_collect_user_data()
        cur.execute("INSERT INTO bulking VALUES (?,?,?,?,?,?)", data_tuple)

    # Commit changes to the db
    print('-' * 45)
    print("\n\tCommitting changes...")
    print('-' * 45)
    con.commit()

except sqlite3.Error as e:
    print("SQLite error:", e)
    print('There was an issue with the database operation.')

finally:
    # Check for recent insert
    print('-' * 45)
    print("\n\tMost recent inserts, descending:")
    print('-' * 45)
    for row in con.execute("SELECT date, weight FROM bulking ORDER BY date"):
        print(row)
    # Close connection to db
    print('-' * 45)
    print("\n\t215Closing the connection to Bulking..")
    print('-' * 45)
    con.close()






