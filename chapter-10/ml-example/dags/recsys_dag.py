
from datetime import datetime



from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from recsys_dag import  _data_is_new, _fetch_dataset, _generate_data_frames, _load_movie_vectors



from airflow import DAG


PG_VECTOR_BACKEND = 'recsys_pg_vector_backend'
S3_HOOK_CONNECTION = 'minio_connection'


# Preload our connections in from the dag



with DAG(
    "movie_rec_sys", start_date=datetime(2023, 1, 1), schedule_interval="@weekly", catchup=False
) as dag:


    data_is_new = BranchPythonOperator(
        task_id = "data_is_new",
        python_callable=_data_is_new
    )

    do_nothing = EmptyOperator(
        task_id = "do_nothing"
    )

    fetch_dataset = PythonOperator(
        task_id = "fetch_dataset",
        python_callable = _fetch_dataset
    )


    generate_data_frames = PythonOperator(
        task_id = "generate_data_frames",
        python_callable = _generate_data_frames
    )

    enable_vector_extension = PostgresOperator(
        task_id="enable_vector_extension",
        postgres_conn_id=PG_VECTOR_BACKEND,
        sql="CREATE EXTENSION IF NOT EXISTS vector;",
    )
    
    load_movie_vectors = PythonOperator(
        task_id="load_movie_vectors",
        python_callable = _load_movie_vectors,
        op_kwargs={'pg_connection_id': PG_VECTOR_BACKEND},
    )
    

    join_no_op = EmptyOperator(
        task_id="join_no_op"
    )

    create_temp_table = PostgresOperator(
        task_id='create_temp_table',
        postgres_conn_id = PG_VECTOR_BACKEND,        
        sql= 'CREATE TABLE "temp" AS TABLE "' + "{{ ti.xcom_pull(key='hash_id', task_ids='data_is_new') }}" + '";'
    )

    swap_prod_table = PostgresOperator(
        task_id='swap_temp_to_prod',
        postgres_conn_id = PG_VECTOR_BACKEND,        
        sql= 'DROP TABLE IF EXISTS "movie_vectors"; ALTER TABLE "temp" RENAME TO "movie_vectors";'
    )




    

data_is_new >> do_nothing
data_is_new >> fetch_dataset >> generate_data_frames

generate_data_frames >> enable_vector_extension >>  load_movie_vectors >> join_no_op
# generate_data_frames >> train_deep_learning_model >> join_no_op

join_no_op >> create_temp_table >> swap_prod_table
# join_no_op >> upload_model_artifact

