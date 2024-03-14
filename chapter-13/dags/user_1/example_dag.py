
from datetime import datetime

from airflow.operators.python import  PythonOperator


from airflow import DAG


def _hello():
    pass


default_args = dict(
    queue ="user_2"
)


with DAG(
    "user_1_dag_1", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False, default_args=default_args
) as dag:

    add_users = PythonOperator(task_id="add_users", python_callable=_hello)
    


