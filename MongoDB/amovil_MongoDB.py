"""
Documentar codigo con Docstring
Path de este directorio /usr/local/airflow/dags/cel_[object Object]
Path de la carpeta Ansible /urs/local/ansible/
"""
import os
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from teco_db.operators.tecoMongoDbOperator import TecoMongoDb
from teco_db.operators.tecoMongoDbOperator import TecoReadMongo


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
#arg
default_args = {
    'owner': 'amovil',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['tambo_core@teco.com.ar'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}
        
#dag
dag = DAG(
dag_id= 'amovil_MongoDB', 
    schedule_interval= None, 
    default_args=default_args
)

#################################### 2) VARIABLES  ####################################

#################################### 3) FUNCTIONS  ####################################

def new_table(**context):
    df = pd.read_csv('/io/cel_amovil/tmp/LTE_AMBA.csv', delimiter = ',' , low_memory=False,skiprows = 1, warn_bad_lines=True, error_bad_lines=False, encoding='latin-1')
    #df = pd.read_csv('/io/cel_amovil/tmp/LTE_AMBA.csv', delimiter = ',')
    _create_df = TecoMongoDb(
    task_id='insert_df',
    source_dataframe = df,
    table='amovil_temp',
    sorce_datatype='dataframe',
    provide_context=True,
    dag=dag
    )
    resultado_create = _create_df.execute()


def lectura(**context):
    objeto = TecoReadMongo(
    task_id='leo',
    table='amovil_temp',
    query = {'*'},
    provide_context=True,
    dag=dag)
    resultado = objeto.execute()

    for item in resultado:
        print (item)


###################################### 1) TASKS  ######################################

_crea_mongoDb = PythonOperator(
    task_id='crea_mongoDb',
    python_callable=new_table,
    dag=dag
    )

_lee_mongoDb = PythonOperator(
    task_id='lee_mongoDb',
    python_callable=lectura,
    dag=dag
    )


#################################### 4) WORKFLOW  #####################################

_crea_mongoDb >> _lee_mongoDb