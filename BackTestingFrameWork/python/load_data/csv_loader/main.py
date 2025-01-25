from datetime import datetime
import os
import pandas as pd
import regex as re


class CSVLoader:
    def __init__(self, folder_path):
        self.folder_path = folder_path
    
    def load_csv_files(self):
        """Load all CSV files from the folder and return a list of tuples (filename, datetime object, dataframe)."""
        dataframes = []
        pattern = r"(\d{2})-(\d{2})-(\d{4})\.csv"
        
        for filename in os.listdir(self.folder_path):
            match = re.match(pattern, filename)
            if match:
                day, month, year = match.groups()
                file_date = datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y")
                file_path = os.path.join(self.folder_path, filename)
                df = pd.read_csv(file_path)
                dataframes.append((filename, file_date, df))
        
        return dataframes

    def process_dataframe(self, df):
        """Clean and preprocess the dataframe."""
        df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df.drop(['Date', 'Time'], axis=1, inplace=True)
        return df
