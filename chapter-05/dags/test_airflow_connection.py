from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_connection():
    import os
    conn_string = os.getenv("AIRFLOW_CONN_MYDB")
    print(f"Connection string: {conn_string}")

dag = DAG(
    'test_env_var_connection',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
)

test_conn_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_connection,
    dag=dag,
)
