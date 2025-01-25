import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from csv_loader.main import CSVLoader
from mongo_db_loader.main import MongoDBHandler




class CSVtoMongoPipeline(CSVLoader, MongoDBHandler):
    def __init__(self, folder_path, db_name, collection_name):
        CSVLoader.__init__(self, folder_path)
        MongoDBHandler.__init__(self, db_name, collection_name)

    def run(self, instrument_name = "BANKNIFTY"):
        """Load CSV, process it, and store it into MongoDB."""
        
        dataframes = self.load_csv_files()

        for filename, file_date, df in dataframes:
            processed_df = self.process_dataframe(df)
            self.insert_data(processed_df, 
                             file_date, 
                             instrument_name)  
        
        self.create_indexes()
