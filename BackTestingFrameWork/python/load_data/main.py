import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from mongo_pipeline.main import CSVtoMongoPipeline



def main():
    folder_path = "/Users/siddhanthmate/Desktop/AllFiles/CODE/WORK_CODE/fintech/DATA/BANKNIFTY"
    db_name = "MARKET_DATA"
    collection_name = "OPTIONS_DATA"

    pipeline = CSVtoMongoPipeline(folder_path, db_name, collection_name)
    pipeline.run()



if __name__ == "__main__":
    main()