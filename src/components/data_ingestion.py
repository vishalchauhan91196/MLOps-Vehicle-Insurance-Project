import os
import sys
from pandas import DataFrame

from sklearn.model_selection import train_test_split

from src.logger import logging
from src.exception import MyException
from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact
from src.data_access.vehicle_data import VehicleData

class DataIngestion():

    def __init__(self, data_ingestion_config: DataIngestionConfig = DataIngestionConfig()):
        try:
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise MyException(e, sys) from e


    def export_data_into_feature_store(self) -> DataFrame:
        try:
            data = VehicleData()
            logging.info(f"Importing data from mongodb")
            df = data.export_collection_as_dataframe(collection_name = self.data_ingestion_config.collection_name)
            logging.info(f"Shape of dataframe: {df.shape}")

            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)

            logging.info(f"Exporting data as a DataFrame into feature store file path: {feature_store_file_path}")
            df.to_csv(feature_store_file_path, index=False, header=True)

            return df

        except Exception as e:
            raise MyException(e, sys) from e


    def train_test_split_data(self, df: DataFrame) -> None:
        try:
            train_set, test_set = train_test_split(df, test_size=self.data_ingestion_config.train_test_split_ratio)
            logging.info("Performed train test split on the dataframe")
            training_file_path = self.data_ingestion_config.training_file_path
            dir_path = os.path.dirname(training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            logging.info(f"Exporting train and test file path.")
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)
            logging.info(f"Exported train and test file path.")

        except Exception as e:
            raise MyException(e, sys) from e     


    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """ Output : train set and test set are returned as the artifacts of data ingestion components. """

        try:
            logging.info("****Starting data ingestion****")
            df = self.export_data_into_feature_store()
            self.train_test_split_data(df)

            data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path, test_file_path=self.data_ingestion_config.testing_file_path)

            logging.info(f"Data ingestion artifact: {data_ingestion_artifact}")

            return data_ingestion_artifact

        except Exception as e:
            raise MyException(e, sys) from e