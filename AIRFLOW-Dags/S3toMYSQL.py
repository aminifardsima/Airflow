import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

# Define S3 bucket and key
bucket_name = 'sima014498663203'
s3_key = 'testdataframes/shop_revenue.csv'  # Path in S3

# Extract the file name from the S3 key (no directory structure, just the filename and extension)
file_name = os.path.basename(s3_key)

# Define local file path with the same name as the S3 file
LOCAL_FILE_PATH = os.path.join('/opt/airflow/dags', file_name)  # Path where file will be downloaded

# Function to download the file from S3
def download_s3_file():
    try:
        # Simulate file download from S3 (replace with actual download logic)
        print(f"Downloading file from S3 to {LOCAL_FILE_PATH}")
        # Example: s3_client.download_file(bucket_name, s3_key, LOCAL_FILE_PATH)
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise

# Function to read the file and send it to MySQL
def send_file_to_mysql():
    try:
        # Read the file using the global LOCAL_FILE_PATH
        df = pd.read_csv(LOCAL_FILE_PATH)

        # Get MySQL connection from Airflow
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')  # Ensure this matches your Airflow connection ID
        engine = mysql_hook.get_sqlalchemy_engine()

        # Overwrite the table if it exists
        df.to_sql('s3data', con=engine, index=False, if_exists='replace')
    except Exception as e:
        print(f"Error processing file and sending to MySQL: {e}")
        raise

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
