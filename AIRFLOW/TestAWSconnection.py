from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def test_s3_connection():
    # Initialize the S3Hook with your connection id ('con01')
    hook = S3Hook(aws_conn_id='con01')
    # Get the underlying boto3 S3 client
    s3_client = hook.get_conn()
    # Call list_buckets on the boto3 client
    response = s3_client.list_buckets()
    # Extract bucket names from the response
    buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
    print("Buckets:", buckets)
    return buckets

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='test_s3_connection',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['test']
) as dag:

    run_test = PythonOperator(
        task_id='run_test_s3_connection',
        python_callable=test_s3_connection
    )

    run_test
