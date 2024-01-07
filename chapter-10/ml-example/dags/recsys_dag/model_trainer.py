import boto3
import botocore
import numpy as np
import polars as pl
import tensorflow as tf
import keras

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split



bucket_name = os.env.get('RECSYS_BUCKET_NAME', 'recsys')
current_run_hash = os.env.get('RUN_HASH',None)
data_set_key = os.env.get('RECSYS_DATA_SET_KEY',None)
endpoint_url = os.env.get('S3_ENDPOINT', 'http://minio:9000')
aws_access_key_id = os.env.get("AWS_ACCESS_KEY", 'airflow123')
aws_secret_access_key = os.env.get("AWS_SECRET_KEY", 'airflow123')

if not current_run_hash or data_set_key:
    raise ValueError(f"CURRENT_RUN_HASH and RECSYS_DATA_SET_KEY environment variables must be set")

def main():

    s3 = boto3.resource('s3',
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    config=Config(signature_version='s3v4')
                    )

    s3.Bucket(bucket_name).download_file(data_set_key, 'ratings.csv')
    

    
    

if __name__ == '__main__':
    main()