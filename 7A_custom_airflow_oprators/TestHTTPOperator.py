from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('example_http_sensor_dag',
         default_args=default_args,
         schedule_interval='@daily',
        is_paused_upon_creation=False,
         catchup=False) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "Starting the DAG"',
    )

    check_http_endpoint = HttpSensor(
        task_id='check_http_endpoint',
        http_conn_id='http_default',
        endpoint='',
        request_params={},
        timeout=20,
        poke_interval=5,
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command='echo "Ending the DAG"',
    )

    start_task >> check_http_endpoint >>  end_task
