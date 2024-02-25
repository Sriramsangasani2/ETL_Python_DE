import logging
import os
from datetime import datetime
from Structured.landing import source_to_landing
from Structured.LoadBronze import landing_to_bronze
from Structured.LoadSilver import bronze_to_silver
from Structured.LoadGold import silver_to_gold
from Services.AzureServices import  AzureDataLakeConnection


# Set up logging
log_file = os.path.join("E:", "log", f"log_{datetime.now().strftime('%Y-%m-%d')}.log")
os.makedirs(os.path.dirname(log_file), exist_ok=True)  # Create log directory if it doesn't exist
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Call the function from my_module
try:

    logging.info("Main Step 1 : Establishing connection with our Source ADLS Gen2")
    connection =AzureDataLakeConnection().establish_connection_with_source()
    logging.info("Main Step 1: Connection with Source ADLS Gen2 Established Successfully")


    logging.info("Step 1: Executing source_to_landing()")
    step1_output = source_to_landing(connection)
    logging.info(f"Step 1 output: {step1_output}")

    if(step1_output['status']=='SUCCESS'):
        logging.info("Step 2: Executing landing_to_bronze()")
        step2_output = landing_to_bronze()
        logging.info(f"Step 2 output: {step2_output}")

    if(step2_output['status']=='SUCCESS' ):
        logging.info("Step 3: Executing bronze_to_silver()")
        step3_output = bronze_to_silver(step2_output['partiton_value'])
        logging.info(f"Step 3 output: {step3_output}")

    if (step3_output['status'] == 'SUCCESS'):
        logging.info("Step 4: Executing silver_to_gold()")
        step4_output = silver_to_gold()
        logging.info(f"Step 4 output: {step4_output}")

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")
