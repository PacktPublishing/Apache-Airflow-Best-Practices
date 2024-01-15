
from datetime import datetime


from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from airflow import DAG


def _setup():
    pass

def _teardown():
    pass

with DAG(
    "my_dag", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False
) as dag:
    
    setup_environment = PythonOperator(
        task_id="setup_environment",
        python_callable=_setup,
    )

    def worker_task_func():
        return "Doing some work!"

    worker_task_obj = PythonOperator(
        task_id="worker_task",
        python_callable=worker_task_func,
    )

    worker_task_obj_2 = PythonOperator(
        task_id="worker_task_2",
        python_callable=worker_task_func,
    )

    teardown_environment = PythonOperator(
        task_id="teardown_environment",
        python_callable=_teardown,
    )

    setup_environment.as_setup() >> [worker_task_obj, worker_task_obj_2] >> teardown_environment.as_teardown()
