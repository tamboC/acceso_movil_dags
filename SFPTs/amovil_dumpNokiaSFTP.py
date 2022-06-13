"""
Este DAG realiza una conexión contra el servidor SFTP del gestor Nokia y 
trae los dumps de la RAN.
Los archivos (5 actualmente, 1 por "G") se actualizan diariamente en el gestor y, con igual periodicidad, el DAG lo replica 
en el directorio local defnido.

"""
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from lib.teco_data_management import *

from datetime import datetime, timedelta
from lib.tambo import *
import pysftp
import yaml

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

#argumentos
default_args = {
    'owner': 'amovil',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['tambo_core@teco.com.ar'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    #'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}
        
#dag
dag = DAG(
dag_id= DAG_ID, 
    #schedule_interval= None, 
    schedule_interval = '0 7 * * *', 
    catchup = False,
    description='SFTP de XMLs Nokia',
    tags = ['SFTP', 'Nokia'],
    default_args=default_args
)


#################################### 2) VARIABLES  ####################################

### Nokia ###

""" En el archivo NokiaSFTP.yml se encuentran los siguientes datos de cofiguración:
    - IPs de hosts
    - Usuarios y contraseñas
    - Paths remoto y local
    - Máscara de los archivos
"""
ruta_Config = '/usr/local/airflow/dags/cel_amovil/Config/NokiaSFTP.yml'

#################################### 3) FUNCTIONS  ####################################
def import_yaml(archivo):
	with open(archivo, 'r') as file:
		Config = yaml.safe_load(file)

	print(f"Datos de configuración: {Config}")

	return Config


def hacerSFTP(**kwargs):

    #Leo datos de configuración globales
    datos_Config = import_yaml(ruta_Config)

    myHostname = datos_Config["myHostname"]
    myUsername = datos_Config["myUsername"]
    myPassword = datos_Config["myPassword"]
    localFilePath = datos_Config["localFilePath"]
    remoteFilePath = datos_Config["remoteFilePath"]
    fileMask = datos_Config["templateArchivoNokia"]
    
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
        print("Connection succesfully stablished ... ")

        # Obtain structure of the remote directory '/var/www/vhosts'
        directory_structure = sftp.listdir()
        print(f"Estructura de directorios: {directory_structure}")
        print(f"Máscara de archivo: {fileMask}")
        for archivo in directory_structure:
            #print(f"Archivo testeado: {archivo}")
            for mascara in fileMask:
                #print(f"Mascara probada:  {mascara}")
                if mascara in archivo:
                    print(f"\tTansfiriendo {remoteFilePath + archivo} a {localFilePath + archivo}")
                    print(f"\t{datetime.now().replace(hour=datetime.now().hour - 3).strftime('%Y-%m-%d %H:%M:%S')}")
                    sftp.get(remoteFilePath + archivo, localFilePath + archivo)
                    print ("\tFin de la transferencia")
                    print(f"\t{datetime.now().replace(hour=datetime.now().hour - 3).strftime('%Y-%m-%d %H:%M:%S')}")
    
# connection closed automatically at the end of the with-block
    


###################################### 1) TASKS  ######################################

SFTP = PythonOperator(
    task_id ='Nokia_SFTP',
    python_callable = hacerSFTP,
    dag=dag
)

    
#################################### 4) WORKFLOW  #####################################

SFTP
