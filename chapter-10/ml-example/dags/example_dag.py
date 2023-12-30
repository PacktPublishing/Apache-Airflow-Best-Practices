
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from example_dag import _choosing_best_model, _train_model

from airflow import DAG

# Generate 3 model training scenarios
train_model_tasks = [
    PythonOperator(
        task_id=f"train_model_{model_id}",
        python_callable=_train_model,
        op_kwargs={"model": model_id},
    )
    for model_id in ["A", "B", "C"]
]

with DAG(
    "my_dag", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False
) as dag:
    choosing_best_model = BranchPythonOperator(
        task_id="choosing_best_model", python_callable=_choosing_best_model
    )
    accurate = BashOperator(task_id="accurate", bash_command="echo 'accurate'")
    inaccurate = BashOperator(task_id="inaccurate", bash_command=" echo 'inaccurate'")

train_model_tasks >> choosing_best_model >> [accurate, inaccurate]
