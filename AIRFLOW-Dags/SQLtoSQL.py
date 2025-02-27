

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
    sql= 'SELECT * FROM simadb.Stuedent'
    result=conn.execute(sql)
    data = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(data, columns=columns)
    return(df)
def transformation(df):
    print(df.columns)  
    # rename_dict = {
    #     '﻿4': 'EnrollmentID', 
    #     '2020001': 'Registration',
    #     'CH101': 'Course_ID',
    #     '1': 'Course_taken',
    #     'A': 'Course_Grade',
    #     '12': 'GradePoints',
    #     'Distinction': 'StudentStatus',
    #     '4': 'StudentCGPA', 
    #     '4_[0]': 'StudentSGPA',
    #     'Pass': 'Course_status'
    # }

    # df.rename(columns=rename_dict, inplace=True) 
    print(df)
    return(df)



def upload_data_to_mysql(df):
    hostname = "localhost"
    username = 'root'
    password = '1234'
    port = 3306
    database = 'simadb'
    engine = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}:{port}/{database}')
    df.to_sql("Stuedent", con=engine, if_exists='replace', index=False)
    engine.dispose()    
    print('Successfully uploaded to MySQL')

with DAG( 
 
 
    dag_id="mysql4",
    schedule="* * * * *",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["airflow34"],
) as dag:  

    get_data=PythonOperator(
        task_id='get_data_from_mysql',
        python_callable=get_data_from_mysql,
    )
    Transform_data= PythonOperator(
        task_id='transformation',
        python_callable=transformation,
        op_args=[get_data.output], 
    )


    upload_data= PythonOperator(
        task_id='upload_data_to_mysql',
        python_callable=upload_data_to_mysql,
        op_args=[get_data.output],  
    )
    get_data>>Transform_data>>upload_data
