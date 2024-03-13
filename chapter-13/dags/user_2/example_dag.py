
from datetime import datetime

from airflow.operators.python import  PythonOperator


from airflow import DAG


def _hello():
    pass



with DAG(
    "user_2_dag_1", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False, queue="user_2"
) as dag:

    add_users = PythonOperator(task_id="add_users", python_callable=_hello)
    


