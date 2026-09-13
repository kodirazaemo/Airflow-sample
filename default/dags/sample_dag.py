from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")
    return "Hello world"

def print_goodbye():
    print("Goodbye from Airflow!")
    return "Goodbye world"

with DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple sample DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['sample'],
) as dag:

    task_1 = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )

    task_2 = BashOperator(
        task_id='bash_task',
        bash_command='echo "Running bash command in Airflow"',
    )

    task_3 = PythonOperator(
        task_id='goodbye_task',
        python_callable=print_goodbye,
    )

    task_1 >> task_2 >> task_3
