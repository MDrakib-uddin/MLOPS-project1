import os
import sys
import pandas as pd
from src.logger import logging
from src.exception import MyException
from src.constants import DATABASE_NAME, MONGODB_URL_KEY
from pymongo import MongoClient

class Proj1Data:
    """
    A class to export MongoDB records as a pandas DataFrame.
    """

    def __init__(self):
        """
        Initializes the MongoDB client connection.
        """
        try:
            self.mongo_client = MongoClient(MONGODB_URL_KEY)
            self.database = self.mongo_client[DATABASE_NAME]
        except Exception as e:
            raise MyException(e, sys)

    def export_collection_as_dataframe(self, collection_name: str) -> pd.DataFrame:
        """
        Exports an entire MongoDB collection as a pandas DataFrame.

        Parameters:
        ----------
        collection_name : str
            The name of the MongoDB collection to export.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the collection data, with '_id' column removed and 'na' values replaced with NaN.
        """
        try:
            logging.info(f"Exporting data from collection: {collection_name}")
            collection = self.database[collection_name]
            df = pd.DataFrame(list(collection.find()))
            
            if "_id" in df.columns:
                df = df.drop("_id", axis=1)
                
            logging.info(f"Exported {len(df)} rows from collection")
            return df
        except Exception as e:
            raise MyException(e, sys)