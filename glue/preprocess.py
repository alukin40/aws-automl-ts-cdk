#!/usr/bin/env python
# coding: utf-8

# Import pandas library for csv import
import zipfile
import boto3
import pandas as pd
import sys
import os
from awsglue.utils import getResolvedOptions

s3 = boto3.client('s3')


def download_and_extract(bucket, fileuri, csv_dir):
    zip_file_name = os.path.join('/tmp', fileuri)
    os.makedirs(os.path.dirname(zip_file_name), exist_ok=True)
    
    s3.download_file(bucket, fileuri, zip_file_name)
        
    try:
        with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
            zip_ref.extractall(csv_dir)
    except FileNotFoundError:
        print(f"{zip_file_name} not found.")
        exit()


def preprocess(csv_dir):
    # Load csv
    try:
        tts_df = pd.read_csv(os.path.join(csv_dir, 'TTS.csv'))
        rts_df = pd.read_csv(os.path.join(csv_dir, 'RTS.csv'))
        metadata_df = pd.read_csv(os.path.join(csv_dir, 'metadata.csv'))
    except FileNotFoundError as e:
        print(f"Error reading file: {e}")
        exit()
        
    # Merge the dataframes
    # 1. Merge TTS.csv with RTS.csv using a right join on ['product_code', 'location_code', 'timestamp']
    merged_data = pd.merge(tts_df, rts_df, how='right', on=['product_code', 'location_code', 'timestamp'])
    
    # 2. Merge the result with metadata.csv using a right join on ['product_code']
    final_data = pd.merge(merged_data, metadata_df, how='right', on=['product_code'])
    
    # Save the merged data to 'training_data.csv'
    final_data.to_csv('input/training_data.csv', index=False)
    
    # you can implement your own preprocessing logic here if you want to

def save_to_s3(bucket, csv_dir):
    single_csv = 'training_data.csv'

    object_key = os.path.join('input/', os.path.basename(single_csv))
    s3.upload_file(os.path.join(csv_dir, single_csv), bucket, object_key)
        

# Main Execution
args = getResolvedOptions(sys.argv, ['bucket', 'fileuri'])
bucket = args['bucket']
fileuri = args['fileuri']

csv_dir = 'input/'
download_and_extract(bucket, fileuri, csv_dir)
preprocess(csv_dir)
save_to_s3(bucket, csv_dir)
