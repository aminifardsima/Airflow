from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Function to send DataFrame to MySQL
def send_dataframe_to_mysql():
    # Create a sample DataFrame
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    }
    df = pd.DataFrame(data)
    
    # Get MySQL connection from Airflow
    mysql_hook = MySqlHook(mysql_conn_id='my_mysql_conn')  # Ensure this matches your Airflow connection ID
    engine = mysql_hook.get_sqlalchemy_engine()

    # Overwrite the table if it exists
    df.to_sql('users', con=engine, index=False, if_exists='replace')  

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# Define DAG
with DAG(
    dag_id='send_dataframe_to_mysql',
    default_args=default_args,
    schedule_interval='@once',  # Run once manually
    catchup=False,
    tags=['mysql', 'dataframe']
) as dag:
    
    # Task to send DataFrame to MySQL
    send_to_mysql = PythonOperator(
        task_id='send_dataframe_to_mysql_task',
        python_callable=send_dataframe_to_mysql
    )

    send_to_mysql
