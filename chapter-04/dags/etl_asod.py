import json
import pathlib

import airflow
import requests
import requests.exceptions as request_exceptions
from datetime import date
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta 

dag_owner = 'Kendrick'

def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    api_key = 'INSERT API KEY'
    url = f'https://api.nasa.gov/planetary/apod?api_key={api_key}'
    response = requests.get(url).json()
    today_image = response['hdurl']
    #download astronomy image of the day
    with open(f'todays_image_{date.today()}.png', 'wb') as f:
        f.write(requests.get(today_image).content)   

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='download_ASOD_image',
        default_args=default_args,
        description='download and notify ',
        start_date =airflow.utils.dates.days_ago(0),
        schedule_interval='@daily',
        catchup=True,
        tags=['None']
):
 
        get_pictures = PythonOperator(
                task_id="get_pictures",
                python_callable=_get_pictures,
        )
 
        notify = BashOperator(
                task_id="notify",
                bash_command='echo "Images for $today_date have been added!"',

        )
 
get_pictures >> notify
 