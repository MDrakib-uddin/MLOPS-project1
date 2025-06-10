import os
import sys
import pandas as pd
from src.logger import logging
from src.exception import MyException
from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact
from src.data_access.proj1_data import Proj1Data

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
            self.proj1_data = Proj1Data()
        except Exception as e:
            raise MyException(e, sys)

    def export_data_into_feature_store(self) -> str:
        """
        Export data from MongoDB to feature store
        """
        try:
            # Create feature store directory
            os.makedirs(self.data_ingestion_config.data_ingestion_dir, exist_ok=True)
            os.makedirs(os.path.join(self.data_ingestion_config.data_ingestion_dir, 
                                   self.data_ingestion_config.data_ingestion_feature_store_dir), 
                       exist_ok=True)

            # Get data from MongoDB
            df = self.proj1_data.export_collection_as_dataframe(
                collection_name=self.data_ingestion_config.collection_name
            )

            # Save data to feature store
            feature_store_file_path = os.path.join(
                self.data_ingestion_config.data_ingestion_dir,
                self.data_ingestion_config.data_ingestion_feature_store_dir,
                self.data_ingestion_config.feature_store_file_path
            )
            
            df.to_csv(feature_store_file_path, index=False)
            logging.info(f"Data exported to feature store at: {feature_store_file_path}")
            
            return feature_store_file_path
        except Exception as e:
            raise MyException(e, sys)

    def split_data_as_train_test(self, feature_store_file_path: str) -> DataIngestionArtifact:
        """
        Split data into train and test sets
        """
        try:
            # Create ingested directory
            ingested_dir = os.path.join(
                self.data_ingestion_config.data_ingestion_dir,
                self.data_ingestion_config.data_ingestion_ingested_dir
            )
            os.makedirs(ingested_dir, exist_ok=True)

            # Read data
            df = pd.read_csv(feature_store_file_path)
            
            # Split data
            train_size = int(len(df) * (1 - self.data_ingestion_config.train_test_split_ratio))
            train_df = df[:train_size]
            test_df = df[train_size:]

            # Save train and test data
            train_file_path = os.path.join(ingested_dir, self.data_ingestion_config.training_file_path)
            test_file_path = os.path.join(ingested_dir, self.data_ingestion_config.testing_file_path)

            train_df.to_csv(train_file_path, index=False)
            test_df.to_csv(test_file_path, index=False)

            logging.info(f"Data split into train and test sets")
            logging.info(f"Train data saved at: {train_file_path}")
            logging.info(f"Test data saved at: {test_file_path}")

            return DataIngestionArtifact(
                feature_store_file_path=feature_store_file_path,
                train_file_path=train_file_path,
                test_file_path=test_file_path
            )
        except Exception as e:
            raise MyException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Start data ingestion process
        """
        try:
            logging.info("Starting data ingestion")
            feature_store_file_path = self.export_data_into_feature_store()
            data_ingestion_artifact = self.split_data_as_train_test(feature_store_file_path)
            logging.info("Data ingestion completed")
            return data_ingestion_artifact
        except Exception as e:
            raise MyException(e, sys)