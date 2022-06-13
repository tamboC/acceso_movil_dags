"""
DAG para guardar el inventario de los sitios conectados a los gestores de Huawei AMBA y MEDI
en las colecciones amovil_centrales y amovil_sitios
"""
####################################### LIBRERIAS #######################################
from datetime import datetime, timedelta
import os
################################### LIBRERIAS AIRFLOW ###################################
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
################################### LIBRERIAS LOCALES ###################################
from lib.teco_data_management import *
from lib.teco_huawei_conn import HuaweiConnector
from lib.teco_mongodb import MongoManager
#########################################################################################


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
dag_id= DAG_ID, 
    schedule_interval = '@daily', 
    catchup = False, 
    default_args=default_args
)


def amba(**kwargs):
    h_amba = HuaweiConnector('10.75.98.19')
    data_centrales, data_sitios = h_amba.get_connected_elements()
    h_amba.disconnection()
    return data_centrales, data_sitios


def medi(**kwargs):
    h_medi = HuaweiConnector('10.75.98.89')
    data_centrales, data_sitios = h_medi.get_connected_elements()
    h_medi.disconnection()
    return data_centrales, data_sitios


def save_db(**kwargs):
    mongo = MongoManager('amovil_centrales')
    mongo.delete_collection()
    mongo = MongoManager('amovil_centrales')
    data_centrales_amba, data_sitios_amba = pull_data(kwargs, 'amba')
    data_centrales_medi, data_sitios_medi = pull_data(kwargs, 'medi')
    mongo.insert_data(data_centrales_amba)
    mongo.insert_data(data_centrales_medi)
    mongo.close_connection()
    mongo = MongoManager('amovil_sitios')
    mongo.delete_collection()
    mongo = MongoManager('amovil_sitios')
    mongo.insert_data(data_sitios_amba)
    mongo.insert_data(data_sitios_medi)
    mongo.close_connection()


_amba = PythonOperator(
    task_id ='amba',
    python_callable = amba,
    dag=dag
)

_medi = PythonOperator(
    task_id ='medi',
    python_callable = medi,
    dag=dag
)

_save_db = PythonOperator(
    task_id ='save_db',
    python_callable = save_db,
    dag=dag
)


[_amba, _medi] >> _save_db