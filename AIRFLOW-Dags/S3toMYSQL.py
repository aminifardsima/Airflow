from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

# S3 and MySQL Configurations
S3_CONN_ID = 'con01'  # Your Airflow S3 connection ID
S3_BUCKET_NAME = 'sima014498663203'
S3_KEY = 'document/Airflow on helm (3)'  # File path in S3
LOCAL_FILE_PATH = f'/tmp/{S3_KEY.split("/")[-1]}'  # Download location

MYSQL_CONN_ID = 'my_mysql_conn'  # Your Airflow MySQL connection ID
MYSQL_TABLE_NAME = 's3_data'  # Name of the table in MySQL


# Function to download the file from S3
def download_s3_file():
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    try:
        hook.get_conn().download_file(S3_BUCKET_NAME, S3_KEY, LOCAL_FILE_PATH)
        print(f"File '{S3_KEY}' downloaded successfully to '{LOCAL_FILE_PATH}'")
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise


# Function to read the file and send it to MySQL
def send_file_to_mysql():
    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"File '{LOCAL_FILE_PATH}' not found. Ensure S3 download was successful.")

    # Read file into a DataFrame (assuming CSV format)
    df = pd.read_csv(LOCAL_FILE_PATH)

    # Connect to MySQL
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    engine = mysql_hook.get_sqlalchemy_engine()

    # Send DataFrame to MySQL (overwrite existing table)
    df.to_sql(MYSQL_TABLE_NAME, con=engine, index=False, if_exists='replace')

    print(f"File data inserted into MySQL table '{MYSQL_TABLE_NAME}' successfully!")


# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# Define DAG
with DAG(
    dag_id='s3_to_mysql',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['s3', 'mysql', 'data_transfer']
) as dag:

    # Task to download file from S3
    download_task = PythonOperator(
        task_id='download_s3_file',
        python_callable=download_s3_file
    )

    # Task to process and send file to MySQL
    send_to_mysql_task = PythonOperator(
        task_id='send_file_to_mysql',
        python_callable=send_file_to_mysql
    )

    # Define task dependencies
    download_task >> send_to_mysql_task
