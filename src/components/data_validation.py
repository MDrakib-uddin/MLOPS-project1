 import json
import sys
import os

import pandas as pd
import yaml

from pandas import DataFrame

from src.exception import MyException
from src.logger import logging
from src.utils.main_utils import read_yaml_file
from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from src.entity.config_entity import DataValidationConfig
from src.constants import SCHEMA_FILE_PATH


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        """
        :param data_ingestion_artifact: Output reference of data ingestion artifact stage
        :param data_validation_config: configuration for data validation
        """
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
        except Exception as e:
            raise MyException(e, sys)

    def validate_all_columns(self, schema_file_path: str, df: pd.DataFrame) -> bool:
        try:
            validation_status = False
            with open(schema_file_path, 'r') as schema_file:
                schema = yaml.safe_load(schema_file)
                all_columns = schema['columns']
                df_columns = df.columns.tolist()
                
                # Check if all required columns are present
                missing_columns = [col for col in all_columns if col not in df_columns]
                if len(missing_columns) == 0:
                    validation_status = True
                else:
                    logging.error(f"Missing columns: {missing_columns}")
                
            return validation_status
        except Exception as e:
            raise MyException(e, sys)

    def is_column_exist(self, df: DataFrame) -> bool:
        """
        Method Name :   is_column_exist
        Description :   This method validates the existence of a numerical and categorical columns
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []
            missing_categorical_columns = []
            for column in self.data_validation_config.numerical_columns:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)

            if len(missing_numerical_columns)>0:
                logging.info(f"Missing numerical column: {missing_numerical_columns}")


            for column in self.data_validation_config.categorical_columns:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)

            if len(missing_categorical_columns)>0:
                logging.info(f"Missing categorical column: {missing_categorical_columns}")

            return False if len(missing_categorical_columns)>0 or len(missing_numerical_columns)>0 else True
        except Exception as e:
            raise MyException(e, sys) from e

    @staticmethod
    def read_data(file_path) -> DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise MyException(e, sys)
        

    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Method Name :   initiate_data_validation
        Description :   This method initiates the data validation component for the pipeline
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """

        try:
            logging.info("Starting data validation")
            
            # Create validation directory if it doesn't exist
            os.makedirs(self.data_validation_config.data_validation_dir, exist_ok=True)
            
            # Read the data
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            
            # Validate columns
            train_validation_status = self.validate_all_columns(
                SCHEMA_FILE_PATH,
                train_df
            )
            test_validation_status = self.validate_all_columns(
                SCHEMA_FILE_PATH,
                test_df
            )
            
            # Create validation report
            validation_report = {
                "train_validation_status": train_validation_status,
                "test_validation_status": test_validation_status,
                "total_columns": len(train_df.columns),
                "columns": train_df.columns.tolist()
            }
            
            # Save validation report
            with open(self.data_validation_config.validation_report_file_path, 'w') as f:
                yaml.dump(validation_report, f)
            
            logging.info("Data validation completed")
            
            return DataValidationArtifact(
                validation_report_file_path=self.data_validation_config.validation_report_file_path,
                validation_status=train_validation_status and test_validation_status
            )
            
        except Exception as e:
            raise MyException(e, sys)