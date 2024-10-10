from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

smtp_user = 'kenvandoorn@gmail.com'

def print_hello():
    return 'Hello World!'

default_args = {
    'owner': 'Kendrick',
    'start_date':datetime(2024,8,18),
}

with DAG(
    dag_id = 'email_alert_example',
    schedule_interval = None,
    default_args = default_args,
) as dag:

    email = EmailOperator(
        task_id = 'email_alert',
        to = 'kenvandoorn@gmail.com',
        subject = 'Email Alert',
        html_content = """ <h3>Email Test</h3>""",
        dag=dag
    )

    dummy_operator = DummyOperator(
        task_id = 'dummy_task',
        retries = 3,
        dag = dag
    )

    hello_operator = PythonOperator(
        task_id = 'hello_task',
        python_callable = print_hello,
        dag = dag
    )

    email >> dummy_operator >> hello_operator