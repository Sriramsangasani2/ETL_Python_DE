from azure.storage.blob import BlobClient, BlobServiceClient
import logging
import configparser
import os
import pandas as pd
from Structured.LoadStrategy.upsert import upsert

config = configparser.ConfigParser()
config.read('D:\\Spark\\ETL\\Config\\config.ini')


def bronze_to_silver(partiton_value):
    try:

        ACCOUNT_NAME = config['storage']['ACCOUNT_NAME']
        ACCOUNT_KEY = config['storage']['ACCOUNT_KEY']

        conn_string = f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};EndpointSuffix=core.windows.net"

        # Blob details
        container_name = 'lakehouse'

        blob_name = config['lakehouse']['RAW'] + '/' + config['dataset']['SUBDOMAIN'] + '/' + partiton_value + '/' + config['dataset']['FILE']

        blob_name_silver = config['lakehouse']['SILVER'] + '/' + config['dataset']['SUBDOMAIN'] +'/' + config['dataset']['FILE']


        # Output blob name
        output_blob_name = config['lakehouse']['SILVER'] + '/' + config['dataset']['SUBDOMAIN'] + '/' + config['dataset']['FILE']

        # Local directory to save the downloaded file
        local_directory = "E:\\lakehouse\\" + config['lakehouse']['RAW'] + "\\" +partiton_value + "\\"  + config['dataset']['SUBDOMAIN']  # Replace with your desired local directory

        # Input file path
        input_file_path = "E:\\lakehouse\\" + config['lakehouse']['RAW'] + "\\" +partiton_value + "\\"  + config['dataset']['SUBDOMAIN'] + "\\" + config['dataset']['FILE']

        target_path = "E:\\lakehouse\\" + config['lakehouse']['SILVER'] + "\\" + config['dataset']['SUBDOMAIN'] + "\\" + config['dataset']['FILE']
        os.makedirs(local_directory, exist_ok=True)

        # Create BlobClient to access the blob
        blob_client = BlobClient.from_connection_string(
            conn_str=conn_string,
            container_name=container_name,
            blob_name=blob_name
        )

        blob_client_silver = BlobClient.from_connection_string(
            conn_str=conn_string,
            container_name=container_name,
            blob_name=blob_name_silver
        )

        # Download the blob and save it to the local directory
        with open(input_file_path, "wb") as local_file:
            download_stream = blob_client.download_blob()
            local_file.write(download_stream.readall())

        with open(target_path, "wb") as local_file:
            download_stream = blob_client_silver.download_blob()
            local_file.write(download_stream.readall())

        source_df = pd.read_csv(input_file_path).astype(str)
        target_df = pd.read_csv(target_path).astype(str)

        final_df=upsert(source_df,target_df)

        final_df.to_csv(target_path, index=False)

        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=output_blob_name)

        # Upload file
        with open(target_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)


        output = {'status': 'SUCCESS', 'silver layer': container_name + '/' + output_blob_name}

        print(f"File successfully transformed and moved from {blob_name}  to  {output_blob_name}.")
        logging.info(f"File successfully transformed and moved from  {blob_name}  to  {output_blob_name}.")

    except Exception as e:
        print(f"An error occurred: {e}")

        logging.error(f"An error occurred while moving file: {str(e)}")
        output = {'status': 'FAILURE'}

    return output

