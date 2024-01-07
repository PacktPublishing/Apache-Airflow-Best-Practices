import logging
import os
import requests
import zipfile

from pathlib import Path

import polars as pl
import numpy as np

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    return S3Hook('recsys_s3')

def __get_pgvector_hook():
    return PostgresHook(postgres_conn_id='recsys_pg_vector_backend')
    
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
    

def __download_and_unpack_dataset(source):

    r = requests.get(source)
    r.raise_for_status()
    logging.info(f"successfully downloaded content from {source}")
    
    zip_dst = Path.home() / Path(source).name
    return_paths = []
    with open(zip_dst,"wb") as f:
        f.write(r.content)
    logging.info(f"External data downloaded to {zip_dst}")

    with zipfile.ZipFile(zip_dst,'r') as z:
        unzip_dst = Path.home() 
        z.extractall(unzip_dst)
        unzip_directory = Path(source).stem
        logging.info(f"Following files unzipped {z.namelist()}")

        # rebuild the absolute path to the files we need to process
        for f in z.namelist():
            if Path(f).name in ['ratings.csv', 'movies.csv']:
                abs_p = Path.home() / Path(f)
                return_paths.append(abs_p)

    logging.info(f"{return_paths} fetched and unpacked ") 
    return return_paths

def _fetch_dataset(ti, **kwargs):
    """
    Downloads the data from the movielens repository and re uploads it to an 
    s3 bucket for future use. 
    """
    
    # Get the source from a variable and generate a local location for the download
    source = Variable.get("DATASET_LOCATION", default_var = DATASET_LOCATION)

    files_to_upload = __download_and_unpack_dataset(source)
    
    for f in files_to_upload:
        s3 = __get_s3_hook()
        bucket = __get_recsys_bucket()  
        hash_id = __get_current_run_hash(ti)
        s3_dst = f"{hash_id}/{Path(f).name}"
        
        
        # we do this because trying to create the same bucket twice will cause a failure. We 
        # prefer to silently pass all errors and catch issues at the load method. 
        try :
            s3.create_bucket(bucket_name = bucket)
        except: 
            pass
            
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
                                  task_ids="fetch_dataset")
    logging.info(ratings_object)
    movies_object = ti.xcom_pull(key="movies.csv",
                                 task_ids="fetch_dataset")
    
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
        ti.xcom_push(key=f"{Path(f).name}.vector_length",
                     value = shape[1]-1)


def _load_movie_vectors(ti):
    s3 = __get_s3_hook()
    bucket = __get_recsys_bucket()
    hash_id = __get_current_run_hash(ti)
    vector_length = ti.xcom_pull(key='movie_watcher_df.parquet.vector_length', task_ids='generate_data_frames')
    
    movies_ratings_object = f"{hash_id}/movie_watcher_df.parquet"

    movie_ratings_file = s3.download_file(
        key = movies_ratings_object,
        bucket_name = bucket
    )

    # esablish postgres connection
    pg_hook = __get_pgvector_hook()

    # drop and re create the table (we do this to manage restarts)
    pg_hook.run(f'DROP TABLE IF EXISTS "{hash_id}";')

    pg_hook.run(f'''CREATE TABLE IF NOT EXISTS "{hash_id}" (          
                    movieId INTEGER PRIMARY KEY,
                    vector VECTOR("{vector_length}")
                );''')
        
    
    def row_generator(df):
        for r in movie_ratings_df.rows():
            yield (r[0], f"{list(r[1:])}")

    movie_ratings_df = pl.read_parquet(movie_ratings_file)    

    #bulk upload
    pg_hook.insert_rows(table=f'"{hash_id}"', rows=(r for r in row_generator(movie_ratings_df)), target_fields=['movieId','vector'])
    pass



    

                