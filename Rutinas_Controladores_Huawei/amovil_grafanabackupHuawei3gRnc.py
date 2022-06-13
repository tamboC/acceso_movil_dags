"""
    Documentar codigo con Docstring
    Path de este directorio /usr/local/airflow/dags/cel_amovil
    Path de la carpeta Ansible /urs/local/ansible/

    Consulta Backups de controladoras 3g Huawei . Hace un print con el estado del bacjup ( si está hecho o no ).
	Introduce en la base influx un Flag , si se realizó backup entonces Flag = 1, si no se hizo backup Flag = 0.
	No se realiza ninguna accion con el backup, ni tampoco se consulta sobre la información de la cual se realizó el backup.Solamente se hizo o no se hizo.

    25 NOV 2021 -- Se agrega BSC
"""
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from lib.teco_data_management import *
from teco_mae_operator.operators.tecoMaeOperator import TecoMaeOperator
from datetime import datetime, timedelta
from lib.L_teco_db import insert_influxdb
    
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
dag_id = 'amovil_grafanabackupHuawei3gRnc', 
    default_args= default_args,
    start_date= datetime(2021,1,1),
    schedule_interval= '00 06 * * *',
    #schedule_interval= None,
    #schedule_interval='@daily', 
    tags = ['Controladores', 'Huawei'],
    catchup = False 
)
#################################### 2) VARIABLES  ####################################

amba = ['USHRNC02',
        'MTDRNC13',
        'ANARNC13',
        'MTDRNC10',
        'ANARNC17',
        'BELRNC03',
        'BELRNC02',
        'BELRNC01',
        'ANARNC14',
        'ANARNC15',
        'BHBRNC03',
        'CLIRNC02',
        'MDPRNC04',
        'ANARNC16',
        'BELRNC04',
        'MUNRNC04',
        'MUNRNC03',
        'MUNRNC02',
        'MDPRNC05',
        'BCHRNC02',
        'MTDRNC12',
        'MTDRNC11',
        'MUNRNC01',
        'MTDRNC14',
        'BSCANA3',
        'BSCMDP4'        
        ]

medi =['LRJRNC02',
        'CTMRNC02',
        'MDZRNC04',
        'CORRNC04',
        'TUCRNC03',
        'RAFRNC01',
        'SNTRNC02',
        'MDZRNC05',
        'SLTRNC03',
        'TUCRNC04',
        'CORRNC05',
        'TUCRNC01',
        'RCURNC01',
        'ANTRNC01',
        'VLLRNC01',
        'ORARNC01',
        'TUCRNC02',
        'SFRRNC01',
        'SLTRNC02',
        'CORRNC01',
        'MTNRNC01',
        'SLTRNC01',
        'LRJRNC01',
        'CORRNC03',
        'SNTRNC01',
        'CTMRNC01',
        'CORRNC02',
        'BSCSFR3',
        'BSCTUC4',
        'BSCANT2',
        'BSCSLT5',
        'BSCALV7',
        'BSCMTN2',
        'BSCCTM2',
        'BSCSNT3',
        'BSCALV8',
        'BSCORA2',
        'BSCLRJ2',
        'BSCRCU2',
        'BSCVLL2',
        'BSCMDZ4',
        'BSCRAF1'
        ]

cmd = ['LST BKPFILE:;']

today = str(datetime.today())

today = today[:10]

#################################### 3) FUNCTIONS  ####################################

def parse_data(**kwargs):
#    print(today)
    output_amba = pull_data(kwargs,'get_data_amba')[0]
    output_medi = pull_data(kwargs,'get_data_medi')[0]
    output_amba.update(output_medi)
    
    timestamp = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    data2 = []
    
    for key,value in output_amba.items():
        flag = 0
#        print(key)
        for linea in value:
#            print(linea)
            if linea.find('.bak') != -1 and today in linea:
                # meter en DB grafana o lo que sea 
                print(key + ': Se encuentra el backup del dia de hoy')
                flag = 1
            elif linea.find('Number of results') != -1 and flag == 0:
                print(key + ': No se encuentra el backup de la fecha: '+ today)

        data2.append({
                    'measurement': 'backupHuawei3gRnc',
                    'tags': {
                            'central_name':key
                            },
                    'time': timestamp,
                    'fields':{
                        'backup': int(flag)
                    }
                })
    insert_influxdb(data2, 'amovil')

def resultados(**kwargs):
    resumen_amba = pull_data(kwargs,'get_data_amba')[1]
    resumen_medi = pull_data(kwargs,'get_data_medi')[1]
    print(resumen_amba)
    print(resumen_medi)
###################################### 1) TASKS  ######################################
# TecoMaeOperator
#task_id='',
#    srv='',
#    elements=,
#    commands=,
#    dag=dag

# Esto ejectura los comandos en los elementos y me devuelve un diccionario donde la key es el sitio
# y el valor es el output del comando, y devuelve un string con el resumen de ejecucion de los comandos

_get_data_amba = TecoMaeOperator (
    task_id ='get_data_amba',
    srv ='10.75.98.19',
    elements=amba,
    commands=cmd,
    dag=dag
)

_get_data_medi = TecoMaeOperator (
    task_id = 'get_data_medi',
    srv ='10.75.98.89',
    elements=medi,
    commands=cmd,
    dag=dag
)

_parse_data = PythonOperator(
    task_id ='parse_data',
    python_callable = parse_data,
    dag=dag
)

_resultados = PythonOperator(
    task_id='resultados',
    python_callable = resultados,
    dag=dag
)

    
#################################### 4) WORKFLOW  #####################################

[_get_data_amba,_get_data_medi] >> _parse_data >>  _resultados
