from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime

import pandas as pd

import os


EXTRACTED_CSV = '/opt/airflow/dags/extracted_persons.csv'

TRANSFORMED_CSV = '/opt/airflow/dags/transformed_persons.csv'



def extract_data_from_mysql():

    hook = MySqlHook(mysql_conn_id='mysql_conn')

    try:

        conn = hook.get_conn()

        cursor = conn.cursor()

        cursor.execute("SELECT id, first_name, last_name FROM persons;")

        rows = cursor.fetchall()


        df = pd.DataFrame(rows, columns=['id', 'first_name', 'last_name'])

        df.to_csv(EXTRACTED_CSV, index=False)

        print(f"Data extracted and saved to {EXTRACTED_CSV}")

    except Exception as e:

        print(f"Error during extraction: {e}")

        raise

    finally:

        cursor.close()

        conn.close()



def transform_data():

    try:

        df = pd.read_csv(EXTRACTED_CSV)


        df['NAME'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')


        df = df[['id', 'NAME']]


        df.to_csv(TRANSFORMED_CSV, index=False)

        print(f"Data transformed and saved to {TRANSFORMED_CSV}")

    except Exception as e:

        print(f"Error during transformation: {e}")

        raise



def load_data_to_mysql():

    hook = MySqlHook(mysql_conn_id='mysql_conn')

    try:

        conn = hook.get_conn()

        cursor = conn.cursor()


        cursor.execute("""

            CREATE TABLE IF NOT EXISTS connect (

                id INT PRIMARY KEY,

                NAME VARCHAR(255)

            );

        """)

        conn.commit()


        df = pd.read_csv(TRANSFORMED_CSV)


        for _, row in df.iterrows():

            cursor.execute(

                "REPLACE INTO connect (id, NAME) VALUES (%s, %s);",

                (int(row['id']), row['NAME'])

            )

        conn.commit()

        print("Data loaded into MySQL table 'connect'.")

    except Exception as e:

        print(f"Error during loading: {e}")

        raise

    finally:

        cursor.close()

        conn.close()



default_args = {

    'owner': 'airflow',

    'start_date': datetime(2024, 1, 1),

}


with DAG(

    dag_id='mysql_etl_one_step',

    default_args=default_args,

    schedule_interval='@once',

    catchup=False,

    tags=['mysql', 'etl', 'multi-step'],

) as dag:


    extract_task = PythonOperator(

        task_id='extract_data_from_mysql',

        python_callable=extract_data_from_mysql,

    )


    

    transform_task = PythonOperator(

        task_id='transform_data',

        python_callable=transform_data,

    )

    load_task = PythonOperator(

        task_id='load_data_to_mysql',

        python_callable=load_data_to_mysql,

    )


    extract_task >> transform_task >> load_task
