import pandas as pd

def upsert(source_df,target_df):

    merged_df = pd.merge(target_df, source_df, on='InvoiceNo', suffixes=('_target', '_source'), how='left')

    target_df['InvoiceNo'] = merged_df['InvoiceNo']
    target_df['StockCode'] = merged_df['StockCode_source'].fillna(merged_df['StockCode_target'])
    target_df['Description'] = merged_df['Description_source'].fillna(merged_df['Description_target'])
    target_df['Quantity'] = merged_df['Quantity_source'].fillna(merged_df['Quantity_target'])
    target_df['InvoiceDate'] = merged_df['InvoiceDate_source'].fillna(merged_df['InvoiceDate_target'])
    target_df['UnitPrice'] = merged_df['UnitPrice_source'].fillna(merged_df['UnitPrice_target'])
    target_df['CustomerID'] = merged_df['CustomerID_source'].fillna(merged_df['CustomerID_target'])
    target_df['Country'] = merged_df['Country_source'].fillna(merged_df['Country_target'])

    new_records_df = source_df[~source_df['InvoiceNo'].isin(target_df['InvoiceNo'])]

    final_df = pd.concat([target_df, new_records_df], ignore_index=True)
    final_df = final_df.sort_values(by='InvoiceNo')

    return final_df



