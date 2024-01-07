import logging
import requests
import zipfile

from pathlib import Path

import polars as pl
import numpy as np

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

DATASET_LOCATION='https://files.grouplens.org/datasets/movielens/ml-latest-small.zip'
DATASET_HASH_LOCATION='https://files.grouplens.org/datasets/movielens/ml-latest-small.zip.md5'



def __get_external_hash(url):
    r = requests.get(url) 
    r.raise_for_status()
    return r.content.decode() \
                    .rstrip() \
                    .split(' = ')[1]

def __get_recsys_bucket():
    return Variable.get("RECSYS_S3_BUCKET",default_var = "recsys") 

def __get_current_run_hash(ti):
    return ti.xcom_pull(key='hash_id',task_ids="data_is_new")  

def __get_s3_hook():
    return S3Hook('recsys_s3_connection')
    
def _data_is_new(ti, xcom_push=False, **kwargs):
    """
    Determine if the external hash of the movielens website are different from the last set we processed.

    If different - store the hash value as an xcom for downstream use.

    Returns:
        str: the next task to execute
    """
    dataset_hash_location = Variable.get("DATASET_HASH_LOCATION", default_var=DATASET_HASH_LOCATION)
    internal_md5 = Variable.get("INTERNAL_MD5", default_var=None)

    external_md5 = __get_external_hash(dataset_hash_location)
    
    if internal_md5 != external_md5 :
        ti.xcom_push(key='hash_id',
                     value=external_md5)
        return 'fetch_dataset'

    return 'do_nothing'
                

def _update_hash_variable(ti,**kwargs):
    """
    updates the INTERNAL_MD5 variable instance to the current runs md5 hash
    """
    Variable.set("INTERNAL_MD5",
                 ti.xcom_pull(key='hash_id',
                              task_ids="data_is_new") )
    

def __download_and_unpack_dataset(dataset):

    r = requests.get(dataset)
    r.raise_for_status()

    with open(local_dst_f,"wb") as f:
        f.write(r.content)

    logging.info(f"External data downloaded to {local_dst_f}")

    with zipfile.ZipFile(local_dst_f,'r') as z:
        z.extractall(Path.home())
        loging.info(f"Following files unzipped {z.namelist()}")

    # # If its one of the two files we need          
    # for f in z.namelist():
    #     if Path(f).name in ['ratings.csv', 'movies.csv']:    
    

    pass

def _fetch_dataset(ti, **kwargs):
    """
    Downloads the data from the movielens repository and re uploads it to an 
    s3 bucket for future use. 
    """
    # Get the source from a variable and generate a local location for the download
    source = Variable.get("DATASET_LOCATION", default_var = DATASET_LOCATION)
    local_dst_f = Path.home() / Path(source).name
    

    files_to_upload = __download_and_unpack_dataset(source)
    
    for f in files_to_upload:
        s3 = __get_s3_hook()
        bucket = __get_recsys_bucket()  
        hash_id = __get_current_run_hash(ti)
        s3_dst = f"{hash_id}/{f}"

        s3.load_file(
            filename=f,
            key=s3_dst,
            bucket_name= bucket,
            replace=True
        )

        ti.xcom_push(key=Path(f).name,
                        value=s3_dst)
    
    
    
        
                



def _process_csv(ratings_csv, movies_csv):
    """
    takes two csv files, turns them into parquet files and puts them to disk. 

    The two data frames are 
        - user_rating_df : each row is a users ratings of all the 
                            films in the catalog
        - movie_watcher_df : each row is a movie and what user ratings
                              were for that movie


    """
     
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
    
    movie_watcher_df.write_parquet( Path.home() / "movie_watcher_df.parquet")
    user_rating_df.write_parquet( Path.home() / "user_rating_df.parquet")

    return  ( Path.home() / "movie_watcher_df.parquet", movie_watcher_df.shape ),\
            ( Path.home() / "user_rating_df.parquet", user_rating_df.shape )

def _generate_data_frames(ti, **kwargs):
    """
    A task that downloads raw CSV files from an S3 bucket, 
    transforms and cleans them into dataframes using polars and 
    stores them back to an S3 bucket as objects. 
    """

    s3 = __get_s3_hook()
    bucket = __get_recsys_bucket()
    ratings_object = ti.xcom_pull(key="ratings.csv",
                                  stask_ids="fetch_datasets")
    movies_object = ti.xcom_pull(key="movies.csv",
                                 task_ids="fetch_datasets")
    
    ratings_csv = s3.download_file(
        key = ratings_object,
        bucket_name = bucket
        )

    movies_csv = s3.download_file(
        key = movies_object,
        bucket_name = bucket
    )

    files_to_upload = _process_csv(ratings_csv=ratings_csv, movies_csv=movies_csv)

    for f, shape in files_to_upload:
        hash_id = ti.xcom_pull(key='hash_id',
                                task_ids="data_is_new")
        s3_dst = f"{hash_id}/{Path(f).name}"

        s3.load_file(
            filename=f,
            key=s3_dst,
            bucket_name= bucket,
            replace=True
            )

        ti.xcom_push(key=Path(f).name,
                     value=s3_dst)
        ti.xcom_push(key=f"{path(f).name}.vector_length",
                     value = shape[1])


def _load_movie_vectors(ti, pg_connection_id):
    s3 = __get_s3_hook()
    bucket = __get_recsys_bucket()
    hash_id = __get_current_run_hash(ti)
    
    movies_ratings_object = f"{hash_id}/movie_watcher_df.parquet"

    movie_ratings_file = s3.download_file(
        key = movies_ratings_object,
        bucket_name = bucket
    )

    # esablish postgres connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    insert_cmd = f"""
                INSERT INTO {hash_id} (movieId, vector)
                VALUES(%s, %s);
                """
    movie_ratings_df = pl.read_parquet(movie_ratings_file)
    # loop through items in doc
    for r in movie_ratings_df.rows():
        row = (r[0], f"'{list(r[1:])}'")
        # insert item to table
        pg_hook.run(insert_cmd, parameters=row)
    pass


def _swap_knn_vector_table():
    pass


def _upload_model_artifact():
    pass
