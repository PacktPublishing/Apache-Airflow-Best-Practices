from datetime import datetime,timedelta

from airflow import DAG

from airflow.models import Connection
from airflow.utils import db


from airflow_provider_tea_pot.operators import MakeTeaOperator,BrewCoffeeOperator
from airflow_provider_tea_pot.sensors import WaterLevelSensor


# Ensure we have a connection
db.merge_conn(
    Connection(
        conn_id="tea_pot_example",
        conn_type="teapot",
        host='tea-pot',
        port='8083',
        extra=dict(pot_designator=0,additions="sugar")
    )
)



default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2021, 7, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "tea-pot-example",
    default_args=default_args,
    description="TeaPot provider examples",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:


    t1 = WaterLevelSensor(
        task_id = "check_water_level",
        tea_pot_conn_id="tea_pot_example",
        minimum_level= 0.2
    )

    t2 = MakeTeaOperator(
        task_id = "make_tea",
        tea_pot_conn_id="tea_pot_example",
    )

    t3 = BrewCoffeeOperator(
        task_id = "brew_coffee",
        tea_pot_conn_id="tea_pot_example",
    )


t1 >> [t2,t3]
