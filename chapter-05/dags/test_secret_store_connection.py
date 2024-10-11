from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook

def test_secret_connection():
    conn = BaseHook.get_connection("mydb") # Replace "mydb" with the actual connection ID from the secret store
    
    print(f"Connection Host: {conn.host}")

dag = DAG(
    'test_secret_store_connection',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
)

test_secret_task = PythonOperator(
    task_id='test_secret_connection',
    python_callable=test_secret_connection,
    dag=dag,
)
