# DAG_TEMPLATE = """
# from datetime import datetime, timedelta
# from airflow import DAG

# from dag_generator import _setup_environment, _teardown_environment

# with DAG(
#     "{{ dag_name }}", 
#     max_active_runs = 1, 
#     is_paused_upon_creation=False, 
#     start_time = datetime.now() - timedelta(days=1)
# ) as dag:

#     setup = PythonOperator(
#         task_id="setup_environment",
#         python_callable=_setup_environment,
#     )


#     teardown = PythonOperator(
#         task_id="teardown_environment",
#         python_callable=_teardown_environment,
#     )

#     setup_environment.as_setup()
#     teardown_environment.as_teardown(setup)
# """

# def _setup_environment(name, resources=None):
#     pass


# def _teardown_environment(name):
#     pass


# def _run_test(endpoint, status_code, ):
#     pass

