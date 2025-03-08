from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

def test_mysql_connection():

    hook = MySqlHook(mysql_conn_id='mysql_conn')

    try:

        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()

        print("Connected to MySQL Server - Version:", version[0])
        return version[0]
    except Exception as e:
        print("MySQL connection failed:", str(e))
        raise e
    finally:
        cursor.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='test_mysql_connection',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['test']
) as dag:

    run_test = PythonOperator(
        task_id='test_mysql_conn',
        python_callable=test_mysql_connection
    )

    run_test
