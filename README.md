# ETL Pipeline using Azure Data Lake Storage Gen2

This repository contains Python scripts for an ETL (Extract, Transform, Load) pipeline using Azure Data Lake Storage Gen2. The pipeline processes data from a source CSV file, stages it in landing, transforms it to bronze, silver, and gold layers, and stores the final data in Azure Data Lake Storage Gen2.

## Repository Structure

- `Structured`: Contains the main ETL scripts.
  - `landing.py`: Moves data from the source to the landing area.
  - `LoadBronze.py`: Transforms data from landing to bronze.
  - `LoadSilver.py`: Transforms data from bronze to silver.
  - `LoadGold.py`: Transforms data from silver to gold.
- `Services`: Contains utility scripts for Azure Data Lake Storage Gen2 connection.
  - `AzureServices.py`: Establishes and manages the connection to Azure Data Lake Storage Gen2.
- `config`: Configuration files.
  - `config.ini`: Contains configuration details such as account name, account key, and paths for various layers.
- `log`: Directory to store log files generated during the ETL process.

## Configuration

Before running the ETL pipeline, make sure to update the `config.ini` file with your Azure Storage account details and paths for landing, bronze, silver, and gold layers.

## Dependencies

- Python 3.x
- Azure Storage SDK: `azure-storage-file-datalake`, `azure-storage-blob`
- Pandas

Install dependencies using:

```bash
pip install azure-storage-file-datalake azure-storage-blob pandas
```

## Running the ETL Pipeline

1. Ensure that the configuration file (`config.ini`) is correctly configured.
2. Run the main script:

```bash
python main.py
```

This will execute the entire ETL pipeline, logging each step's status. Check the logs in the `log` directory for details.

## Note

- The ETL pipeline assumes a CSV data format for the source file.
- The `upsert` function in `LoadSilver.py` handles updating existing records and inserting new records.
- Make sure to replace placeholder values in the configuration file with your actual Azure Storage account details.

