# =========================================================
# Filename:     aws-dynamodb-upload.py
# Description:  Lambda code to upload dataframe to DynamoDB
# =========================================================
'''
Assumes the Lambda function has lambda-layer for pandas and numpy.
'''

# standard AWS Lambda python packages
import boto3
from io import StringIO
from operator import attrgetter

# Lambda Layer (layer-python37)
import pandas as pd
import numpy as np
pd.set_option("display.max_columns", 999)


def upload_df_to_dynamodb(df_upload, dyn_db_table):
    '''
    Upload pandas dataframe to DynamoDB table.
    Also delete existing items from table before uploads.
    
    Inputs:
        df_upload - dataframe, upload to DynamoDB
        dyn_db_table - str, name of DynamoDB table (setup beforehand)
    
    Convert pandas dataframe to DynamoDB:
    http://oksoft.blogspot.com/2017/02/import-csv-data-file-to-dynamodb.html
    '''
    
    # Clean-up the data, change column types to strings to be on safer side :)
    df_upload=df_upload.fillna(0)
    for i in df_upload.columns:
        df_upload[i] = df_upload[i].astype(str)
        
    # Convert dataframe to list of dictionaries (JSON) that can be consumed by any no-sql database
    ls_json = df_upload.T.to_dict().values()
    
    print(ls_json)
    
    # instantiate boto3-dynamodb resource
    dynamodb_resource = boto3.resource('dynamodb')
    
    # Connect to the DynamoDB table
    dynamodb_table = dynamodb_resource.Table(dyn_db_table)
    
    # delete existing items in table
    dynamodb_scan = dynamodb_table.scan()
    with dynamodb_table.batch_writer() as batch:
        for each_record in dynamodb_scan['Items']:
            batch.delete_item(Key={'OptionSymbol': each_record['OptionSymbol']})
    
    print(f'Cleared records in DynamoDB table: {dyn_db_table}')
    
    # Load the JSON object created in the step 3 using put_item method
    for each_json in ls_json:
        dynamodb_table.put_item(Item=each_json)
    
    print(f'Uploaded records to DynamoDB table: {dyn_db_table}')
