import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime



class MongoDBHandler:
    def __init__(self, db_name, collection_name, uri="mongodb://localhost:27017/"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

  
    def insert_data(self, df, file_date, instrument_name):
        """Insert processed data into MongoDB with efficient schema design."""
        operations = []

        for _, row in df.iterrows():
            operations.append(UpdateOne(
                {
                    'ticker': row['Ticker'],
                    'expiry': row['Expiry'],
                    'strike': row['Strike'],
                    'file_date': file_date,  
                    'instrument_name': instrument_name  
                },
                {
                    '$set': {
                        'contract_monthly': row['Contract_Monthly'],
                        'contract_weekly': row['Contract_Weekly']
                    },
                    '$push': {
                        'data': {
                            'datetime': row['datetime'],
                            'open': row['Open'],
                            'high': row['High'],
                            'low': row['Low'],
                            'close': row['Close'],
                            'volume': row['Volume'],
                            'oi': row['OI'],
                            'type': row['Type'],
                            'script': row['Script']
                        }
                    }
                },
                upsert=True
            ))
            
        self.collection.bulk_write(operations)


    def create_indexes(self):
        """Create indexes for faster query performance."""
        self.collection.create_index([("ticker", 1), ("expiry", 1), ("strike", 1), ("data.datetime", 1)])
        print("Indexes created for fast retrieval.")
