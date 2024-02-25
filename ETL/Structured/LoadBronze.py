from azure.storage.blob import BlobClient, BlobServiceClient
import logging
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read('D:\\Spark\\ETL\\Config\\config.ini')



def landing_to_bronze():

    try:
        current_datetime = datetime.now()
        parition_value = current_datetime.strftime("%Y%m%d%H%M%S")
        ACCOUNT_NAME = config['storage']['ACCOUNT_NAME']
        ACCOUNT_KEY = config['storage']['ACCOUNT_KEY']
        conn_string = f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};EndpointSuffix=core.windows.net"

        # Blob details
        container_name = 'lakehouse'
        blob_name =config['lakehouse']['LANDING'] + '/' + config['dataset']['SUBDOMAIN'] + '/' + config['dataset']['FILE']
        # Output blob name
        output_blob_name = config['lakehouse']['RAW'] + '/' + config['dataset']['SUBDOMAIN'] + '/' + parition_value + '/' + config['dataset']['FILE']
        #output_blob_name = "raw/sales/2020202020/sales.csv"

        # Local directory to save the downloaded file
        local_directory = "E:\\lakehouse\\" + config['lakehouse']['LANDING'] + "\\" + config['dataset']['SUBDOMAIN']  # Replace with your desired local directory

        # Input file path
        input_file_path = "E:\\lakehouse\\" + config['lakehouse']['LANDING'] + "\\" + config['dataset']['SUBDOMAIN'] + "\\" + config['dataset']['FILE']

        # Create BlobClient to access the blob
        blob_client = BlobClient.from_connection_string(
            conn_str=conn_string,
            container_name=container_name,
            blob_name=blob_name
        )


        # Download the blob and save it to the local directory
        #with open(f"{local_directory}/sales.csv", "wb") as local_file:
        with open(input_file_path, "wb") as local_file:
            download_stream = blob_client.download_blob()
            local_file.write(download_stream.readall())

        # Delete the blob
        blob_client.delete_blob()

        # Create BlobServiceClient to interact with the blob service
        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=output_blob_name)

        # Upload file
        with open(input_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        output = {'status': 'SUCCESS', 'bronze layer': container_name+'/'+output_blob_name, 'partiton_value':parition_value}

        logging.info(f"File successfully transformed and moved from  {blob_name}  to  {output_blob_name}.")

        print(f"File successfully transformed and moved from  {blob_name}  to  {output_blob_name}.")

    except Exception as e:
        print(f"An error occurred: {e}")

        logging.error(f"An error occurred while moving file: {str(e)}")
        output = {'status': 'FAILURE'}

    return output
