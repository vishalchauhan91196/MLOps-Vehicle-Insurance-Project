from src.cloud_storage.aws_storage import SimpleStorageService
from src.exception import MyException
from src.entity.estimator import MyModel
import sys
from pandas import DataFrame


class VehicleDataEstimator:
    """ Handles saving/loading ML model from S3 and performing predictions. """

    def __init__(self, bucket_name: str, model_path: str):
        self.bucket_name = bucket_name
        self.model_path = model_path
        self.s3 = SimpleStorageService()
        self.loaded_model: MyModel = None


    def is_model_present(self, model_path: str) -> bool:
        """ Check if model exists in S3 bucket. """
        try:
            return self.s3.s3_key_path_available(
                bucket_name=self.bucket_name,
                s3_key=model_path
            )
        except MyException as e:
            print(e)
            return False


    def load_model(self) -> MyModel:
        """ Load model from S3. """
        try:
            return self.s3.load_model(
                model_name=self.model_path,
                bucket_name=self.bucket_name
            )
        except Exception as e:
            raise MyException(e, sys) from e


    def save_model(self, local_model_file: str, remove: bool = False) -> None:
        """ Upload a trained model from local filesystem to S3. """
        try:
            self.s3.upload_file(
                local_path=local_model_file,
                bucket_path=self.model_path,
                bucket_name=self.bucket_name,
                remove=remove
            )
        except Exception as e:
            raise MyException(e, sys) from e


    def predict(self, dataframe: DataFrame):
        """ Perform prediction using loaded model. """
        try:
            # Lazy loading: load only when needed.
            if self.loaded_model is None:
                self.loaded_model = self.load_model()

            return self.loaded_model.predict(dataframe=dataframe)

        except Exception as e:
            raise MyException(e, sys) from e
