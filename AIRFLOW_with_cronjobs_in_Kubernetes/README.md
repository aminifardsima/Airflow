##Step1:
####Write an Airflow code that already works. 
`vim code.py`
Python Code:
```

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


```
##Step 2: 
####Because in our example we are connected to mysql database then to simulate it, we install sql on another pod. Remember that this is an example but its better to make a service for our mysql database instead of making it by pod!.  Running it by a service will make it running all the time.
`Kubectl create ns simaproject`
`vim mysqlserver.yaml`

YAML FILE:
```
apiVersion: v1
kind: Pod
metadata:
  name: mysql-container
  namespace: simaproject
spec:
  containers:
    - name: mysql
      image: mysql:5.7
      env:
        - name: MYSQL_ROOT_PASSWORD
          value: "1234" # Root password for MySQL
        - name: MYSQL_DATABASE
          value: "simadb" # Pre-created database
      ports:
        - containerPort: 3306 # MySQL port
      resources:
        requests:
          memory: "256Mi"
          cpu: "500m"
        limits:
          memory: "512Mi"
          cpu: "1000m"

```

`kubectl apply -f mysqlserver.yaml -n simaproject` 
`kubectl po -n simaproject`

##Step3:
####We make a file named `requirements.txt` in order to call libraries that are used in the python code.

Python Code:
```python
pandas
sqlalchemy
pymysql
apache-airflow
```
`kubectl apply -f requirements.yaml -n simaproject`

Step4:

Inside the code.py change the name of the code like this to make it accessible to the container exists on your local host:
`vim code.py`

```

    
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
def get_data_from_mysql():
    import pandas as pd
    from sqlalchemy import create_engine

    hostname = "mysql-container.simaproject.svc.cluster.local"  # Use the Kubernetes service DNS
    username = 'root'
    password = '1234'
    port = 3306
    database = 'simadb'

    engine = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}:{port}/{database}')
    conn = engine.connect()
    sql = 'SELECT * FROM simadb.tradedataTB'
    result = conn.execute(sql)
    data = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(data, columns=columns)
    return df

def upload_data_to_mysql(df):
    import pandas as pd
    from sqlalchemy import create_engine

    hostname = "mysql-container.simaproject.svc.cluster.local"  # Use the Kubernetes service DNS
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
                               
```


Step5:
 
We make a Dokerfile  in order to get ready to make an image out of our code( for those working in minikube must put their image into minikube)
Use a base Python image 
`vim Dockerfile`


```
FROM python:3.9-slim

#### Set the working directory inside the container
WORKDIR /app

##### Copy your Python script (code.py) into the container
COPY code.py /app/

##### Optionally, if your Python script has dependencies, create a requirements.txt file and add it here
##### COPY requirements.txt /app/
##### RUN pip install -r requirements.txt

##### Install required dependencies, such as Airflow, MySQL libraries, and any other libraries in your script
RUN pip install pandas sqlalchemy pymysql apache-airflow

##### Set the command to run your Python script when the container starts
CMD ["python", "code.py"]
```


`docker build -t my-python-etl-image:latest`
Step6:

 in this step we make an etl-cronjob.yaml file in order to make our cronjob and run it to have forexample 2 pods at the same time applying the code simultaneously.
`vim etl-cronjob.yaml`
Yaml FILE:
```
apiVersion: batch/v1
kind: CronJob
metadata:
  name: etl-cronjob
  namespace: simaproject
spec:
  schedule: "*/5 * * * *"  # Runs every 5 minutes
  jobTemplate:
    spec:
      parallelism: 2  # Number of Pods to run in parallel
      completions: 2  # Number of completions required
      activeDeadlineSeconds: 300  # Timeout for each job (5 minutes)
      template:
        spec:
          nodeSelector:
            kubernetes.io/hostname: minikube  # Use the 'minikube' node name
          containers:
            - name: python-worker
              #image: 192.168.49.1:31326/my-python-etl-image:latest # Updated with Minikube IP
              image: my-python-etl-image:latest
              imagePullPolicy: Never
                #imagePullPolicy: IfNotPresent  # Try to pull only if not already present
              env:
                - name: MYSQL_HOST
                  value: "mysql-container.simaproject.svc.cluster.local"  # MySQL service hostname
                - name: MYSQL_PORT
                  value: "3306"
                - name: MYSQL_USER
                  value: "root"
                - name: MYSQL_PASSWORD
                  value: "1234"
                - name: MYSQL_DATABASE
                  value: "simadb"
              resources:
                requests:
                  memory: "256Mi"  # Adjust based on your actual needs
                  cpu: "100m"      # Further reduced CPU request (0.1 CPU cores)
                limits:
                  memory: "512Mi"  # Limit memory usage
                  cpu: "200m"      # Further reduced CPU limit (0.2 CPU cores)
          restartPolicy: Never  # Pods will not restart once the script completes
            #imagePullSecrets:
            #- name: my-registry-secret
                                                
```

 `kubectl apply -f etl-cronjob.yaml -n simaproject`






