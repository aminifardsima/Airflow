
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
def get_data_from_mysql():
    hostname="localhost"
    username='root'
    password='1234'
    port=3306
    database='simadb'
    engine = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}:{port}/{database}')
    conn= engine.connect()
    sql= 'SELECT * FROM simadb.tradedataTB'
    result=conn.execute(sql)
    data = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(data, columns=columns)
    return(df)
def transformation(df):
    
def upload_data_to_mysql(df):
    hostname = "localhost"
    username = 'root'
    password = '1234'
    port = 3306
    database = 'simadb'
    engine = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}:{port}/{database}')
    df.to_sql("tradedataTBnew", con=engine, if_exists='replace', index=False)
    engine.dispose()    
    print('Successfully uploaded to MySQL')

with DAG( 
 
 
    dag_id="etl_pipeline_from_mysql",
    schedule="* * * * *",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["etl_by_airflow"],
) as dag:  

    get_data=PythonOperator(
        task_id='get_data_from_mysql',
        python_callable=get_data_from_mysql,
    )

    upload_data= PythonOperator(
        task_id='upload_data_to_mysql',
        python_callable=upload_data_to_mysql,
        op_args=[get_data.output],  # Pass the output of get_data_task as an argument
    )
    get_data>>upload_data

