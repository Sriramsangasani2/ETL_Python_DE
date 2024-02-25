import pandas as pd

source_df = pd.read_csv("E:\\lakehouse\\silver\\sales1.csv").astype(str)
target_df = pd.read_csv("E:\\lakehouse\\silver\\sales\\sales.csv").astype(str)
# Assuming you have source_df and target_df DataFrames

# Step 1: Update existing records in target_df
# Merge source_df and target_df based on a common key, such as 'InvoiceNo'
merged_df = pd.merge(target_df, source_df, on='InvoiceNo', suffixes=('_target', '_source'), how='left')
target_df['InvoiceNo'] = merged_df['InvoiceNo']
target_df['StockCode'] = merged_df['StockCode_source'].fillna(merged_df['StockCode_target'])
target_df['Description'] = merged_df['Description_source'].fillna(merged_df['Description_target'])
target_df['Quantity'] = merged_df['Quantity_source'].fillna(merged_df['Quantity_target'])
target_df['InvoiceDate'] = merged_df['InvoiceDate_source'].fillna(merged_df['InvoiceDate_target'])
target_df['UnitPrice'] = merged_df['UnitPrice_source'].fillna(merged_df['UnitPrice_target'])
target_df['CustomerID'] = merged_df['CustomerID_source'].fillna(merged_df['CustomerID_target'])
target_df['Country'] = merged_df['Country_source'].fillna(merged_df['Country_target'])


# Step 2: Insert new records from source_df to target_df
# Append rows from source_df that are not already present in target_df
new_records_df = source_df[~source_df['InvoiceNo'].isin(target_df['InvoiceNo'])]

target_df = pd.concat([target_df, new_records_df], ignore_index=True)
# Now target_df contains updated existing records and newly inserted records

# You can optionally sort the DataFrame by 'InvoiceNo' after updating and inserting records
target_df = target_df.sort_values(by='InvoiceNo')

# Finally, you can save the updated target_df to a CSV file
target_df.to_csv("E:\\lakehouse\\silver\\sales\\sales1.csv", index=False)


