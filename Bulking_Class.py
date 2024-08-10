import sqlite3
import pandas as pd
import re
import numpy as np

class BulkingDataPipeline:
    def __init__(self, db_name):
        self.db_name = db_name
        self.con = sqlite3.connect(db_name)
        self.cur = self.con.cursor()
    
    def __del__(self):
        self.con.close()
    
    def clean_numerical(self, weight):
        """
        Cleans numerical data by removing non-numeric characters.
        
        Parameters:
        weight (str): The string containing the weight data.
        
        Returns:
        str: The cleaned numerical data.
        """
        try:
            return re.sub(r'[^\d.]+', '', weight)
        except ValueError as e:
            print("An error occurred during weight cleaning:")
            return np.nan
    
    def ingest_raw_data(self, csv_path, raw_table_name):
        try:
            # Load new data from CSV
            new_data = pd.read_csv(csv_path)

            # Insert new rows into raw table
            new_data.to_sql(raw_table_name, self.con, if_exists='append', index=False)
            print("New rows inserted into the raw table.")
        except Exception as e:
            print("An error occurred during raw data ingestion:", e)
    
    def transform_and_ingest(self, raw_table_name, silver_table_name):
        try:
            # Load data from raw table
            raw_data = pd.read_sql_query(f"SELECT * FROM {raw_table_name}", self.con)

            # Perform transformations and cleaning
            raw_data['Date'] = pd.to_datetime(raw_data['Date'], format='%b%d%Y', errors='coerce')
            raw_data['weight(lbs)'] = raw_data['weight(lbs)'].astype(str).apply(self.clean_numerical).astype(float).round(1)
            raw_data['workout_days'] = raw_data['workout_days'].astype(float).round(1)
            raw_data['missed_meals'] = raw_data['missed_meals'].astype(str).apply(self.clean_numerical).astype(float).round(1)
            raw_data['protein'] = raw_data['protein'].astype(int)
            raw_data['creatine'] = raw_data['creatine'].astype(int)

            # Insert transformed data into silver table
            raw_data.to_sql(silver_table_name, self.con, if_exists='replace', index=False)
            print("Data transformed and inserted into the silver table.")
        except AttributeError as e:
            print('There was an error with the transformation function:', e)
        except sqlite3.OperationalError as e:
            print('There was an error with the SQLite operation:', e)
        except Exception as e:
            print("An error occurred during data transformation:", e)
            # Print out problematic rows for debugging
            problematic_rows = raw_data[pd.to_numeric(raw_data['Weight(lbs)'], errors='coerce').isnull()]
            if not problematic_rows.empty:
                print("Problematic rows in 'Weight(lbs)' column:")
                print(problematic_rows)

    def run_pipeline(self, csv_path, raw_table_name, silver_table_name):
        self.ingest_raw_data(csv_path, raw_table_name)
        self.transform_and_ingest(raw_table_name, silver_table_name)

# Create congifuration file for the pipeline
config = {
    "db_name": "bulking_database.db",
    "csv_path": "/Users/geraldlittlejr/Documents/vs_files/bulking/bulking_file.txt",
    "raw_table_name": "bulking_database",
    "silver_table_name": "bulking_table_silver"
}

# Example usage:
pipeline = BulkingDataPipeline("bulking_database.db")
pipeline.run_pipeline("/Users/geraldlittlejr/Documents/vs_files/bulking/bulking_file.txt", "bulking_database", "bulking_table_silver")
