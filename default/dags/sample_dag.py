from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
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
    dag_id='sample_dag',
    default_args=default_args,
    description='A simple sample DAG',
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=1),
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
