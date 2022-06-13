"""
Este DAG realiza una conexión contra los servidores de SFTP de los gestores Huawei (MAE) y 
trae los dumps de la RAN.
Los archivos (2, 1 por gestor) se actualizan diariamente en el gestor y, con igual periodicidad, el DAG lo replica 
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
    schedule_interval = '0 2 * * *', 
    catchup = False,
    description='SFTP de XMLs Huawei',
    tags = ['SFTP', 'Huawei'],
    default_args=default_args
)


#################################### 2) VARIABLES  ####################################

### Huawei ###
""" En el archivo HuaweiSFTP.yml se encuentran los siguientes datos de cofiguración:
    - IPs de hosts
    - Dominio (AMBA / MEDI)
    - Usuarios y contraseñas
    - Paths remoto y local
    - Máscara de los archivos
"""
ruta_Config = '/usr/local/airflow/dags/cel_amovil/Config/HuaweiSFTP.yml'

#################################### 3) FUNCTIONS  ####################################
def import_yaml(archivo):
	with open(archivo, 'r') as file:
		Config = yaml.safe_load(file)


	print(Config)
	print(f"Dato tipo: {type(Config)}")
	print(Config["myHostname"])

	return Config

def hacerSFTP(**kwargs):

    fileMask = datetime.now().strftime('%m_%d_%Y')
    
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    #Leo datos de configuración globales
    datos_Config = import_yaml(ruta_Config)

    primarioActivo = datos_Config["workingPrimario"]
    myUsername = datos_Config["myUsername"]
    myPassword = datos_Config["myPassword"]
    localFilePath = datos_Config["localFilePath"]
    remoteFilePath = datos_Config["remoteFilePath"]
    templateArchivoHuawei = datos_Config["templateArchivoHuawei"]

    #for host in myHostname:
    for host in datos_Config["myHostname"]:

        host_ip = host["ip"]
        host_mae = host["mae"]


        if primarioActivo == host["primario"]:
            print(f"Connecting to MAE {host_mae} - {host_ip}...")
            with pysftp.Connection(host=host_ip, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
                print(f"Connection succesfully stablished to MAE {host_mae} - {host_ip}")

                # Switch to a remote directory
                sftp.cwd(remoteFilePath)
                # Obtain structure of the remote directory '/var/www/vhosts'
                directory_structure = sftp.listdir()
                print(f"Estructura de directorios: {directory_structure}")
                for archivo in directory_structure:
                    print(archivo)
                    if fileMask in archivo:
                        print(f"\tTansfiriendo {remoteFilePath + archivo} a {localFilePath + host_mae}_{templateArchivoHuawei}")
                        print(f"\t{datetime.now().replace(hour=datetime.now().hour - 3).strftime('%Y-%m-%d %H:%M:%S')}")
                        sftp.get(remoteFilePath + archivo, localFilePath + host_mae + "_" + templateArchivoHuawei)
                        print ("\tFin de la transferencia")
                        print(f"\t{datetime.now().replace(hour=datetime.now().hour - 3).strftime('%Y-%m-%d %H:%M:%S')}")
        
    # connection closed automatically at the end of the with-block
    


###################################### 1) TASKS  ######################################

SFTP = PythonOperator(
    task_id ='Huawei_SFTP',
    python_callable = hacerSFTP,
    dag=dag
)

    
#################################### 4) WORKFLOW  #####################################

SFTP
