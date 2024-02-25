from azure.storage.blob import BlobClient, BlobServiceClient
import logging
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('D:\\Spark\\ETL\\Config\\config.ini')


def silver_to_gold():

    try:

        ACCOUNT_NAME = config['storage']['ACCOUNT_NAME']
        ACCOUNT_KEY = config['storage']['ACCOUNT_KEY']

        conn_string = f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};EndpointSuffix=core.windows.net"

        # Blob details
        container_name = 'lakehouse'
        blob_name =config['lakehouse']['SILVER'] + '/' + config['dataset']['SUBDOMAIN'] + '/' +  config['dataset'][
            'FILE']

        output_blob_name = config['lakehouse']['GOLD'] + '/' + config['dataset']['SUBDOMAIN'] + '/'+  config['dataset'][
            'FILE']

        # Local directory to save the downloaded file
        local_directory = "E:\\lakehouse\\" + config['lakehouse']['SILVER'] + "\\" + config['dataset'][
            'SUBDOMAIN']  # Replace with your desired local directory

        # Input file path
        input_file_path = "E:\\lakehouse\\" + config['lakehouse']['SILVER'] + "\\" + config['dataset'][
            'SUBDOMAIN'] + "\\" + config['dataset']['FILE']

        target_file_path = "E:\\lakehouse\\" + config['lakehouse']['GOLD'] + "\\" + config['dataset'][
            'SUBDOMAIN'] + "\\" + config['dataset']['FILE']


        # Output blob name


        # Create BlobClient to access the blob
        blob_client = BlobClient.from_connection_string(
            conn_str=conn_string,
            container_name=container_name,
            blob_name=blob_name
        )

        # Download the blob and save it to the local directory
        with open(input_file_path, "wb") as local_file:
            download_stream = blob_client.download_blob()
            local_file.write(download_stream.readall())



        # Read the data from CSV file
        df = pd.read_csv(input_file_path)

        # Aggregate based on CustomerID and InvoiceNo, summing the Quantity
        aggregated_df = df.groupby(['CustomerID', 'InvoiceNo']).agg({'Quantity': 'sum'}).reset_index()
        aggregated_df['CustomerID'] = aggregated_df['CustomerID'].astype(int)
        aggregated_df.to_csv(target_file_path, index=False)


        # Create BlobServiceClient to interact with the blob service
        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=output_blob_name)

        # Upload file
        with open(target_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        output = {'status': 'SUCCESS', 'gold layer': container_name+'/'+output_blob_name}

        print(f"File successfully transformed and moved from  {blob_name} to {output_blob_name}.")
        logging.info(f"File successfully transformed and moved from  {blob_name}  to  {output_blob_name}.")

    except Exception as e:
        print(f"An error occurred: {e}")

        logging.error(f"An error occurred while moving file: {str(e)}")
        output = {'status': 'FAILURE'}

    return output

