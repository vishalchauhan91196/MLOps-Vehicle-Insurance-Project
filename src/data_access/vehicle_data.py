import sys
import pandas as pd
import numpy as np
from typing import Optional

from src.exception import MyException
from src. constants import DATABASE_NAME
from src. configuration import MongoDBClient

class VehicleData:
    """  Class to export the data fetched from MongoDB to a Pandas DataFrame.  """

    def __init__(self) -> None:
        try:
            """  Initializes the MongoDB client connection.  """
            self.mongoclient = MongoDBClient(database_name = DATABASE_NAME)
            
        except Exception as e:
            raise MyException(e, sys)

    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        """  Exports an entire MongoDB collection as a pandas DataFrame.  """

        try:
            if database_name is None:
                collection = self.mongoclient.database[collection_name]
            else:
                collection = self.mongoclient[database_name][collection_name]

            # Convert collection data to DataFrame and preprocess
            print("Fetching data from mongoDB")
            df = pd.DataFrame(list(collection.find()))
            print(f"Data fecthed with len: {len(df)}")
            if "id" in df.columns:
                df.drop(columns=["id"], errors="ignore")
            df.replace({"na": np.nan}, inplace=True)

            return df

        except Exception as e:
            raise MyException(e, sys)      