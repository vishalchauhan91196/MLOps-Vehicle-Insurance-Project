import boto3
import os, sys
import pickle

from io import StringIO
from typing import Union,List
from botocore.exceptions import ClientError
from pandas import DataFrame,read_csv

from src.configuration.aws_connection import S3Client
from src.logger import logging
from src.exception import MyException

class SimpleStorageService:
    """
    A class for interacting with AWS S3 using the boto3 client interface.
    """

    def __init__(self):
        """
        Initializes the SimpleStorageService instance with S3 client.
        """
        s3_client = S3Client()
        self.s3_client = s3_client.s3_client

    # ------------------------------------------------------------
    # CHECK IF KEY EXISTS
    # ------------------------------------------------------------
    def s3_key_path_available(self, bucket_name: str, s3_key: str) -> bool:
        """
        Checks if a specified S3 key exists.
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise MyException(e, sys)

    # ------------------------------------------------------------
    # READ OBJECT
    # ------------------------------------------------------------
    def read_object(self, bucket_name: str, key: str, decode: bool = True, make_readable: bool = False):
        """
        Reads an S3 object.
        """
        try:
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            body = obj["Body"].read()

            if decode:
                body = body.decode()

            if make_readable:
                return StringIO(body)

            return body

        except Exception as e:
            raise MyException(e, sys) from e

    # ------------------------------------------------------------
    # GET FILE OBJECT (returns bytes)
    # ------------------------------------------------------------
    def get_file_object(self, bucket_name: str, prefix: str) -> List[str]:
        """
        Lists objects matching a prefix and returns their keys.
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix
            )

            if "Contents" not in response:
                return []

            return [item["Key"] for item in response["Contents"]]

        except Exception as e:
            raise MyException(e, sys) from e

    # ------------------------------------------------------------
    # LOAD MODEL
    # ------------------------------------------------------------
    def load_model(self, model_name: str, bucket_name: str, model_dir: str = None):
        """
        Loads a pickled model from S3.
        """
        try:
            key = f"{model_dir}/{model_name}" if model_dir else model_name

            model_bytes = self.read_object(bucket_name, key, decode=False)
            model = pickle.loads(model_bytes)

            logging.info(f"Model '{model_name}' loaded successfully from S3.")
            return model

        except Exception as e:
            raise MyException(e, sys) from e

    # ------------------------------------------------------------
    # CREATE FOLDER
    # ------------------------------------------------------------
    def create_folder(self, bucket_name: str, folder_name: str):
        """
        Creates a folder in S3 (S3 does not support real folders).
        """
        try:
            folder_key = folder_name.rstrip("/") + "/"

            # Put empty object to imitate folder
            self.s3_client.put_object(Bucket=bucket_name, Key=folder_key)

            logging.info(f"Folder '{folder_key}' created successfully.")

        except Exception as e:
            raise MyException(e, sys) from e

    # ------------------------------------------------------------
    # UPLOAD FILE
    # ------------------------------------------------------------
    def upload_file(self, local_path: str, bucket_path: str, bucket_name: str, remove: bool = True):
        """
        Uploads a local file to S3.
        """
        try:
            logging.info(f"Uploading {local_path} to s3://{bucket_name}/{bucket_path}")

            self.s3_client.upload_file(local_path, bucket_name, bucket_path)

            if remove:
                os.remove(local_path)

            logging.info("Upload completed successfully.")

        except Exception as e:
            raise MyException(e, sys) from e

    # ------------------------------------------------------------
    # UPLOAD DF AS CSV
    # ------------------------------------------------------------
    def upload_df_as_csv(self, df: DataFrame, local_path: str, bucket_path: str, bucket_name: str):
        """
        Uploads a DataFrame as CSV to S3.
        """
        try:
            df.to_csv(local_path, index=False)
            self.upload_file(local_path, bucket_path, bucket_name)
        except Exception as e:
            raise MyException(e, sys) from e

    # ------------------------------------------------------------
    # READ CSV FROM S3 INTO DATAFRAME
    # ------------------------------------------------------------
    def get_df_from_object(self, bucket_name: str, key: str) -> DataFrame:
        """
        Reads an S3 object into a pandas DataFrame.
        """
        try:
            csv_buffer = self.read_object(bucket_name, key, decode=True, make_readable=True)
            return read_csv(csv_buffer)
        except Exception as e:
            raise MyException(e, sys) from e

    def read_csv(self, filename: str, bucket_name: str) -> DataFrame:
        """
        Reads CSV from S3 using filename.
        """
        try:
            return self.get_df_from_object(bucket_name, filename)
        except Exception as e:
            raise MyException(e, sys) from e