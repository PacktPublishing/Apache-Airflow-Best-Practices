import boto3
import botocore
import numpy as np
import polars as pl
import tensorflow as tf
import keras
import os

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split



run_hash = os.environ.get('RUN_HASH',None)
data_set_key = os.environ.get('RECSYS_DATA_SET_KEY',None)

bucket_name = os.environ.get('RECSYS_BUCKET_NAME', 'recsys')
endpoint_url = os.environ.get('S3_ENDPOINT', 'http://minio:9000')
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY", 'airflow123')
aws_secret_access_key = os.environ.get("AWS_SECRET_KEY", 'airflow123')

if not (run_hash and data_set_key):
    raise ValueError(f"RUN_HASH and RECSYS_DATA_SET_KEY environment variables must be set")

def main():

    s3 = boto3.resource('s3',
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                    )

    s3.Bucket(bucket_name).download_file(data_set_key, 'ratings.csv')
    

    
    

if __name__ == '__main__':
    main()