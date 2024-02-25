import logging
import configparser
import os
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from datetime import datetime

class AzureDataLakeConnection:
    def __init__(self):
        try:
            self.config = configparser.ConfigParser()
            self.config.read('D:\\Spark\\ETL\\Config\\config.ini')

            self.ACCOUNT_NAME = self.config['storage']['ACCOUNT_NAME']
            self.ACCOUNT_KEY = self.config['storage']['ACCOUNT_KEY']
        except Exception as e:
            logging.error(f"An error occurred while reading configuration: {str(e)}")
            raise

    def establish_connection_with_source(self):
        try:
            service_client = DataLakeServiceClient(account_url=f"https://{self.ACCOUNT_NAME}.dfs.core.windows.net",
                                                    credential=self.ACCOUNT_KEY)
            logging.info("Azure Data Lake connection established successfully.")
            return service_client
        except Exception as e:
            logging.error(f"An error occurred while establishing Azure Data Lake connection: {str(e)}")
            raise


