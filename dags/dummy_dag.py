from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello_world():
    print('Hello world')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dummy_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    print_hello_world_task = PythonOperator(
        task_id='print_hello_world_task',
        python_callable=print_hello_world
    )

    
    end_task = DummyOperator(
        task_id='end'
    )

    start_task >> print_hello_world_task >> end_task
