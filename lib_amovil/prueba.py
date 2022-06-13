"""
Este es un DAG de prueba, para probar levantar una librería propia.
""" 

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from lib.teco_data_management import *
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator

#from lisy_plugin.operators.lisy_operator import LisyCheckTokenOperator, LisyModPort, LisyQueryPort, LisyQueryOops, LisyQueryCorporateService, LisyQueryVlan, LisyRelacion
#from lisy_plugin.operators.lisy_operator import *
#from lisy_plugin.operators.lisy_operator_2 import LisyRelacion

from datetime import datetime, timedelta
import logging

#from lib.funcionesIGA import *
#import sys
#sys.path.insert(0, '/cel_amovil/airflow/dags/lib_amovil')
#from funcionesIGA import *

#arg
default_args = {
    'owner': 'amovil',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
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
    dag_id='amovil_pruebaLucianoLib_2', 
    schedule_interval= None, 
    description='Pruebas de librerias externas',
    default_args=default_args
)

#variables

datoPrueba = "dato no definido"

#tasks
def preparoDatos(**kwargs):
    #datoPrueba = timestamp = datetime.now()
    datoPrueba = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    datoPrueba2 = datetime.now().replace(hour=datetime.now().hour - 3).strftime('%Y-%m-%d %H:%M:%S')
    print(datoPrueba)
    #return datoPrueba
    push_data(kwargs, "dato1", datoPrueba)
    push_data(kwargs, "dato2", datoPrueba2)

def imprimoDatos(**kwargs):
    info = pull_data(kwargs,"t1","dato1")
    #info = pull_data(kwargs,"t1")
    print('info: ', info)
    #info = pull_data(kwargs,"t1")
    info = pull_data(kwargs,"t1","dato2")
    print('info: ', info)
    #info = pull_data(kwargs,"t1")
    print('info: ', info)
   
# prueba ambiente UAT

t1 = PythonOperator(
    task_id ='t1',
    python_callable = preparoDatos,
    dag=dag
)

t2 = PythonOperator(
    task_id ='t2',
    python_callable = imprimoDatos,
    dag=dag
)


##################

t1 >> t2