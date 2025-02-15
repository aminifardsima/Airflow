from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Define a simple Python function
def print_hello():
    print("Hello sima!")

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='simple_airflow_dag',
    default_args=default_args,
    description='A simple DAG example',
    schedule_interval=None,  # Run on demand
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    # Tasks
    start = DummyOperator(task_id='start')
    hello_task = PythonOperator(task_id='hello_task', python_callable=print_hello)
    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> hello_task >> end
