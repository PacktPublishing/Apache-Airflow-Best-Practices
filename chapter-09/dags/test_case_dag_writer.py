from datetime import datetime
from jinja2 import Environment, Template
import json
import os
from random import randint, choice


from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


from airflow import DAG


TEST_CASE_CONN_ID = "test_cases_backend"

dag_template = """
import time
import random
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TEST_CASE_CONN_ID = "test_cases_backend"

def _setup():
    print("Setting up your test environment now")
    return

def _teardown():
    print("Tearing down your test environment now")
    return

def _test_case(s):
    time.sleep(s)
    if random.uniform(0, 1) > 0.995:
        raise Exception("The test failed.")
    return 

def _mark_success():
    pg_hook = PostgresHook(postgres_conn_id=TEST_CASE_CONN_ID)
    
    update_query = '''
        UPDATE "cases"
        SET case_status= 'SUCCESS'
        WHERE case_id = {{dag_id}} ;
    '''
    # Execute the update query
    pg_hook.run(update_query)

tests = []

with DAG(
 "test_case_{{dag_id}}",
 is_paused_upon_creation = False,
 start_date=datetime.datetime(2021, 1, 1),
 catchup=False
) as dag:

    setup_environment = PythonOperator(
        task_id="setup_environment",
        python_callable=_setup,
    )

    teardown_environment = PythonOperator(
        task_id="teardown_environment",
        python_callable=_teardown,
    )

    {% for task in tasks %}
    tests.append(PythonOperator(
        task_id = "{{task.name}}",
        python_callable = _test_case,
        op_args = [{{task.value}}]
        ))
    {% endfor %}

    mark_successful = PythonOperator(
        task_id="mark_succesful",
        python_callable= _mark_success,
    )

    
setup_environment.as_setup() >> tests >> teardown_environment.as_teardown()
tests >> mark_successful
"""



def _generate_dags():
    pg_hook = PostgresHook(postgres_conn_id=TEST_CASE_CONN_ID)

    results = pg_hook.get_records('SELECT case_id, case_descriptor FROM "cases" WHERE "case_status" is NULL ')
    
    for r in results:
    
        with open(os.path.join("/opt","airflow","dags", f"case_{r[0]}_dag.py"),"w") as f:
            env = Environment()
            template = env.from_string(dag_template)
            output = template.render(dict(
                dag_id = r[0],
                tasks = json.loads(r[1])
            ))
            f.write(output)
    

def _drop_successful_dags():
    pg_hook = PostgresHook(postgres_conn_id=TEST_CASE_CONN_ID)

    results = pg_hook.get_records('SELECT case_id FROM "cases" WHERE "case_status" = \'SUCCESS\'; ')
    
    for r in results:
        try:
            os.unlink(os.path.join("/opt","airflow","dags", f"case_{r[0]}_dag.py"))
        except FileNotFoundError:
            pass




with DAG(
    "e2e_test_generator",
    start_date=datetime(2021, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False,
) as dag:
    
    generate_dags = PythonOperator(
        task_id = "generate_dags",
        python_callable = _generate_dags
    )

    drop_successful_dags = PythonOperator(
        task_id = "drop_successful_dags",
        python_callable = _drop_successful_dags
    )

generate_dags
drop_successful_dags
