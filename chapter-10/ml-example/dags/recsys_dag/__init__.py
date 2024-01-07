import logging
import requests
import zipfile

import polars as pl
import numpy as np
from airflow.hooks.S3_hooks import S3Hook

DATASET_LOCATION='https://files.grouplens.org/datasets/movielens/ml-latest-small.zip'
DATASET_HASH_LOCATION='https://files.grouplens.org/datasets/movielens/ml-latest-small.zip.md5'



def _data_is_new(ti, xcom_push=False, **kwargs):
    """
    Determine if the external hash of the movielens website are different from the last set we processed.

    If different - store the hash value as an xcom for downstream use.

    Returns:
        str: the next task to execute
    """
    dataset_hash_location = VARIABLE.get("DATASET_HASH_LOCATION", default_var=DATASET_HASH_LOCATION)
    internal_md5 = Variable.get("INTERNAL_MD5", default_var=None)

    r = requests.get(dataset_hash_location)
    if r.ok:
        external_md5 = r.content.decode() \
                        .rstrip() \ 
                        .split(' = ')[1]

        if internal_md5 != external_md5 :
            ti.xcom_push(key='hash_id',
                         value=external_md5)
            return 'download_dataset'

    return 'do_nothing'
                

def _update_hash_variable(ti,**kwargs):
    """
    updates the INTERNAL_MD5 variable instance to the current runs md5 hash
    """
    Variable.set("INTERNAL_MD5",
                 ti.xcom_pull(key='hash_id',
                              task_ids="data_is_new") )
    


def _fetch_dataset(ti, **kwargs):
    """
    Downloads the data from the movielens repository and re uploads it to an 
    s3 bucket for future use. 
    """
    # Get the source from a variable and generate a local location for the download
    source = Variable.get("DATASET_LOCATION")
    local_dst_f = Path.home() / Path(source).name
    
    # Download our zip file from movie lens
    r = requests.get(source)
    if r.ok:
        with open(local_dst_f,"wb") as f:
            f.write(r.content)

        # Extract the zip file 
        with zipfile.ZipFile(local_dst_f,'r') as z:
            z.extractall(Path.home())

            # If its one of the two files we need
            
            for f in z.namelist():
                if Path(f).name in ['ratings.csv', 'movies.csv']:    
                    #Upload it to s3 at /<hash_id>/file_name.csv
                    s3 = S3Hook('recsys_s3_connection')
                    bucket = Variable.get("RECSYS_S3_BUCKET")        
                    hash_id = ti.xcom_pull(key='hash_id',
                                           task_ids="data_is_new")
                    s3_dst = f"{hash_id}/{f}}"

                    s3.load_file(
                        filename=f,
                        key=s3_dst,
                        bucket_name= bucket,
                        replace=True
                    )

                    ti.xcom_push(key=Path(f).name,
                                 value=s3_dst)
    else:
        raise ValueError(f"Download request failed with {r.status_code}")

def _generate_data_frames(ti, **kwargs):
    """
    A task that downloads raw CSV files from an S3 bucket, 
    transforms and cleans them into dataframes using polars and 
    stores them back to an S3 bucket as objects. 

    The two data frames are 
        - user_rating_df : each row is a users ratings of all the films in the catalog
        - movie_watcher_df : each row is which users have watched that movie

    Depending on the size of your data and operations your taking,
    it might be a better idea to do this computation "outside" of airflow using specialized
    infrastructure like a job scheduler, spark cluster, etc. 

    """

    s3 = S3Hook('recsys_s3_connection')
    bucket = Variable.get("RECSYS_S3_BUCKET")        
    ratings_object = ti.xcom_pull(key="ratings.csv",
                                  stask_ids="fetch_datasets")
    movies_object = ti.xcom_pull(key="movies.csv",
                                 task_ids="fetch_datasets")
    
    ratings_csv = s3.download_file(
        key = ratings_object
        bucket_name = bucket
    )    

    movies_csv = s3.download_file(
        key = movies_object,
        bucket_name = bucket
    )

    # create initial dataframes, drop un needed columns
    ratings_df = pl.read_csv(ratings_csv)
    ratings_df.drop_in_place("timestamp")
    movies_df = pl.read_csv(movies_csv)
    movies_df.drop_in_place("genres")

    
    # Join, and pivot to create our user rating matrix.
    user_rating_df = ratings_df.join(movies_df, on="movieId")
    user_rating_df = user_rating_df \
                        .pivot(index="userId",columns="movieId",values="rating") \ 
                        .fill_null(0)
    
    # Explicit Memory Management
    user_rating_df.shrink_to_fit()
    del ratings_df
    del movies_df

    movie_watcher_df = user_rating_df.drop('userId') \
                .transpose(include_header=True,header_name = "movieId") \
                .with_columns(pl.col("movieId") \
                .cast(pl.Int32) \
                .alias("movieId"))
    
    movie_watcher_df.shrink_to_fit()


def _create_knn_vector_table():
    pass


def _swap_knn_vector_table():
    pass


def _upload_model_artifact():
    pass
