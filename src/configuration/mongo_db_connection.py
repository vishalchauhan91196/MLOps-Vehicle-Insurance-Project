import os
import sys
import pymongo
import certifi

from src.logger import logging
from src.exception import MyException
from src.constants import DATABASE_NAME, MONGODB_URL_KEY

# Load the certificate authority file to avoid timeout errors when connecting to MongoDB
ca = certifi.where()

class MongoDBClient:
    """  MongoDBClient is responsible for establishing a connection to the MongoDB database.  """

    # Shared MongoDBClient instance across all MongoDBClient instances
    client = None
    
    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        """  Initializes a connection to the MongoDB database. If no existing connection is found, it establishes a new one.  """
        try:
            # Check if no MongoDB client connection has been established
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                if mongo_db_url is None:
                    raise Exception(f'Environment Variable {MONGODB_URL_KEY} is not set.')
                
                # Establish a new MongoDB client connection
                MongoDBClient.client = pymongo.MongoDBClient(mongo_db_url, tlsCAFile=ca)

            self.client = MongoDBClient.client
            self.data_base = self.client[database_name]
            self.database_name = database_name
            logging.info("MongoDB connection successful.")

        except Exception as e:
            raise MyException(e, sys)        