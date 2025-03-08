import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

bucket_name = 'sima014498663203'
s3_key = 'testdataframes/shop_revenue.csv'  


file_name = os.path.basename(s3_key)


LOCAL_FILE_PATH = os.path.join('/opt/airflow/dags', file_name)  


def download_s3_file():
    try:
        print(f"Downloading file from S3 to {LOCAL_FILE_PATH}")

    except Exception as e:
        print(f"Error downloading file: {e}")
        raise

def send_file_to_mysql():
    try:

        df = pd.read_csv(LOCAL_FILE_PATH)

        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')  
        engine = mysql_hook.get_sqlalchemy_engine()


        df.to_sql('s3data', con=engine, index=False, if_exists='replace')
    except Exception as e:
        print(f"Error processing file and sending to MySQL: {e}")
        raise


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
