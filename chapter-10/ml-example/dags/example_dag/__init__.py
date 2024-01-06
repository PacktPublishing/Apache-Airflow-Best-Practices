import logging
import requests
import zipfile

from airflow.models.taskinstance import TaskInstance
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
        external_md5 = r.content.decode().rstrip().split(' = ')[1]

        if internal_md5 != external_md5 :
            ti.xcom_push(key='hash_id',value=external_md5)
            return 'download_dataset'

    return 'do_nothing'
                

def _update_hash_variable(ti,**kwargs):
    """
    updates the INTERNAL_MD5 variable instance to the current runs md5 hash
    """
    Variable.set("INTERNAL_MD5", ti.xcom_pull(key='hash_id', task_ids="data_is_new") )
    


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
            rv = []
            for f in z.namelist():
                if Path(f).name in ['ratings.csv', 'movies.csv']:    
                    #Upload it to s3 at /<hash_id>/file_name.csv
                    s3 = S3Hook('recsys_s3_connection')
                    bucket = Variable.get("RECSYS_S3_BUCKET")        
                    hash_id = ti.xcom_pull(key='hash_id',task_ids="data_is_new")
                    s3_dst = f"{hash_id}/{f}}"

                    s3.load_file(
                        filename=f,
                        key=s3_dst,
                        bucket_name= bucket,
                        replace=True
                    )
                    rv.push(s3_dst)
        #return the uploaded destinations
        return s3_dst

        
        
    
        
        
    

        


    return s3_dst


def _generate_data_sets():
    pass


def _create_knn_vector_table():
    pass


def _swap_knn_vector_table():
    pass


def _upload_model_artifact():
    pass
