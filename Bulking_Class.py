import sqlite3
import pandas as pd
import re

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
        return re.sub(r'[^\d.]+', '', weight)
    
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
            raw_data['Weight(lbs)'] = raw_data['Weight(lbs)'].astype(str).apply(self.clean_numerical).astype(float).round(1)
            raw_data['Workout_days'] = raw_data['Workout_days'].astype(float).round(1)
            raw_data['Missed_meals'] = raw_data['Missed_meals'].astype(float).round(1)
            raw_data['Protein'] = raw_data['Protein'].astype(int)
            raw_data['Creatine'] = raw_data['Creatine'].astype(int)

            # Insert transformed data into silver table
            raw_data.to_sql(silver_table_name, self.con, if_exists='replace', index=False)
            print("Data transformed and inserted into the silver table.")
        except Exception as e:
            print("An error occurred during data transformation:", e)

    
    def run_pipeline(self, csv_path, raw_table_name, silver_table_name):
        self.ingest_raw_data(csv_path, raw_table_name)
        self.transform_and_ingest(raw_table_name, silver_table_name)

# Example usage:
pipeline = BulkingDataPipeline("bulking_database.db")
pipeline.run_pipeline("/Users/geraldlittlejr/Documents/vs_files/bulking/bulking_file.txt", "bulking_table", "bulking_table_silver")
