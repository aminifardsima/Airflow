from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def download_s3_file():

    hook = S3Hook(aws_conn_id='AWSconn')


    bucket_name = 'sima014498663203'
    s3_key = 'testdataframes/shop_revenue.csv'


    local_file_path = f'/opt/airflow/dags/{s3_key.split("/")[-1]}'

    try:

        hook.get_conn().download_file(bucket_name, s3_key, local_file_path)
        print(f"File '{s3_key}' downloaded successfully to '{local_file_path}'")
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='download_s3_file',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['s3', 'download']
) as dag:

    download_task = PythonOperator(
        task_id='download_file',
        python_callable=download_s3_file
    )

    download_task
