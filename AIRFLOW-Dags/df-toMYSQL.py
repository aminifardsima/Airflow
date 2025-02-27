from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


def send_dataframe_to_mysql():
   
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    }
    df = pd.DataFrame(data)
    
   
    mysql_hook = MySqlHook(mysql_conn_id='my_mysql_conn') 
    engine = mysql_hook.get_sqlalchemy_engine()

   
    df.to_sql('users', con=engine, index=False, if_exists='replace')  


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}


with DAG(
    dag_id='send_dataframe_to_mysql',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False,
    tags=['mysql', 'dataframe']
) as dag:
    
   
    send_to_mysql = PythonOperator(
        task_id='send_dataframe_to_mysql_task',
        python_callable=send_dataframe_to_mysql
    )

    send_to_mysql
