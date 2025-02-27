from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os


S3_CONN_ID = 'con01'  
S3_BUCKET_NAME = 'sima014498663203'
S3_KEY = 'document/Airflow on helm (3)' 
LOCAL_FILE_PATH = f'/tmp/{S3_KEY.split("/")[-1]}'  

MYSQL_CONN_ID = 'my_mysql_conn' 
MYSQL_TABLE_NAME = 's3_data'  



def download_s3_file():
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    try:
        hook.get_conn().download_file(S3_BUCKET_NAME, S3_KEY, LOCAL_FILE_PATH)
        print(f"File '{S3_KEY}' downloaded successfully to '{LOCAL_FILE_PATH}'")
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise



def send_file_to_mysql():
    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"File '{LOCAL_FILE_PATH}' not found. Ensure S3 download was successful.")


    df = pd.read_csv(LOCAL_FILE_PATH)


    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    engine = mysql_hook.get_sqlalchemy_engine()


    df.to_sql(MYSQL_TABLE_NAME, con=engine, index=False, if_exists='replace')

    print(f"File data inserted into MySQL table '{MYSQL_TABLE_NAME}' successfully!")



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}


with DAG(
    dag_id='s3_to_mysql',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['s3', 'mysql', 'data_transfer']
) as dag:

   
    download_task = PythonOperator(
        task_id='download_s3_file',
        python_callable=download_s3_file
    )

 
    send_to_mysql_task = PythonOperator(
        task_id='send_file_to_mysql',
        python_callable=send_file_to_mysql
    )

    
    download_task >> send_to_mysql_task
