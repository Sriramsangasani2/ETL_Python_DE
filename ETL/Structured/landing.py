import logging
import configparser


config = configparser.ConfigParser()
config.read('D:\\Spark\\ETL\\Config\\config.ini')

def source_to_landing(service_client):

    try:

        # Specify the source and destination paths
        SOURCE_PATH = config['dataset']['SOURCE'] + '/' + config['dataset']['SUBDOMAIN'] + '/' + config['dataset']['FILE']
        DESTINATION_PATH = 'lakehouse/' + config['lakehouse']['LANDING'] + '/' + config['dataset']['SUBDOMAIN'] + '/' + config['dataset']['FILE']

        logging.info(f"Source path: {SOURCE_PATH}, Destination path: {DESTINATION_PATH}")

        source_file_system_client = service_client.get_file_system_client(file_system=SOURCE_PATH.split("/")[0])


        source_file_client = source_file_system_client.get_file_client(SOURCE_PATH[len(SOURCE_PATH.split("/")[0]) + 1:])
        destination_file_system_client = service_client.get_file_system_client(file_system=DESTINATION_PATH.split("/")[0])

        source_file_client.rename_file(DESTINATION_PATH)
        output = {'status': 'SUCCESS', 'landing path': DESTINATION_PATH}

        logging.info(f"File successfully moved from source: {SOURCE_PATH}  to landing {DESTINATION_PATH}.")
        print(f"File successfully moved from {SOURCE_PATH}  to {DESTINATION_PATH}.")

    except Exception as e:
        logging.error(f"An error occurred while moving file: {str(e)}")
        output = {'status': 'FAILURE'}

    return output
