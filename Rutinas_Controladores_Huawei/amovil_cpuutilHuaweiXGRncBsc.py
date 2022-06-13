"""
    DAG CPU Utilization Huawei BSC RNC AMBA / MEDI.
    Datos en base influx
    Grafico en grafafna
    Schedule = @hora
    
    Documentar codigo con Docstring
    Path de este directorio /usr/local/airflow/dags/cel_amovil
    Path de la carpeta Ansible /urs/local/ansible/
"""
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from lib.teco_data_management import *
from teco_mae_operator.operators.tecoMaeOperator import TecoMaeOperator
from datetime import datetime, timedelta
from lib.L_teco_db import insert_influxdb
#Importación de librerias de amovil
import sys
sys.path.insert(0, '/usr/local/tambo/cels/cel_amovil/airflow2/dags/lib_amovil/')
from IpGestoresMAE import get_IpGestoresMae
from get_inventario import get_central

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
    #'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}
        
#dag
dag = DAG(
dag_id = DAG_ID, 
    schedule_interval='@hourly', 
    catchup = False,
    tags = ['Controladores', 'Huawei'],
    default_args=default_args
)

#################################### 2) VARIABLES  ####################################

amba6910, medi6910, medi6900 = get_central()

#carga cpu
cmd6900 = ['DSP CPUUSAGE:SRN=0;', 'DSP CPUUSAGE:SRN=1;' , 'DSP CPUUSAGE:SRN=2;']#carga cpu
cmd6910 = ['DSP CPUUSAGE:SRN=0;', 'DSP CPUUSAGE:SRN=1;' , 'DSP CPUUSAGE:SRN=2;']#carga cpu

Slot = ""#carga cpu

Subsystem_No = ""#carga cpu

CPU_occupancy = ""#carga cpu
#carga cpu

#################################### 3) FUNCTIONS  ####################################
ipGestor = get_IpGestoresMae()

def parse_data(**kwargs):
#    print(today)
    output_amba = pull_data(kwargs,'get_data_amba6910')[0]
    output_medi = pull_data(kwargs,'get_data_medi6910')[0]
    output_amba.update(output_medi)
    output_medi = pull_data(kwargs,'get_data_medi6900')[0]
    output_amba.update(output_medi)
    
    timestamp = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    data1 = []

    for key,value in output_amba.items():
        for linea in value:
            if 'CPU occupancy' in linea:
                cpu_index = linea.index('CPU occupancy')
            if linea[:3].strip().isdigit():
                Slot = linea[1:3].strip()
                Subsystem = linea[11:13].strip()
                try:
                    CPU_occupancy = int(linea[cpu_index:cpu_index+5].strip()[:-1])
                except:
                    CPU_occupancy = 0
                print(CPU_occupancy)
                data1.append({
                    'measurement': 'cargacpuHuawei3gRnc',
                    'tags': {
                            'central_name':key,
                            'Slot': Slot,
                            'Subsystem_No': Subsystem
                            },
                    'time': timestamp,
                    'fields':{
                        'CPU_occupancy': int(CPU_occupancy)
                          
                    }
                })
    insert_influxdb(data1, 'amovil')


def resultados(**kwargs):
    resumen_amba_6910 = pull_data(kwargs,'get_data_amba6910')[1]
    resumen_medi_6910 = pull_data(kwargs,'get_data_medi6910')[1]
    resumen_medi_6900 = pull_data(kwargs,'get_data_medi6900')[1]
    print(resumen_amba_6910)
    print(resumen_medi_6910)
    print(resumen_medi_6900)
###################################### 1) TASKS  ######################################
# TecoMaeOperator
#task_id='',
#    srv='',
#    elements=,
#    commands=,
#    dag=dag

# Esto ejectura los comandos en los elementos y me devuelve un diccionario donde la key es el sitio
# y el valor es el output del comando, y devuelve un string con el resumen de ejecucion de los comandos

_get_data_amba6910 = TecoMaeOperator(
    task_id ='get_data_amba6910',
    srv =ipGestor['AMBA'],     #SLO
    #srv = '10.75.102.5',      #HOR
    elements=amba6910,
    commands=cmd6910,
    dag=dag
)

_get_data_medi6910 = TecoMaeOperator(
    task_id = 'get_data_medi6910',
    srv =ipGestor['MEDI'],
    elements=medi6910,
    commands=cmd6910,
    dag=dag
)

_get_data_medi6900 = TecoMaeOperator(
    task_id = 'get_data_medi6900',
    srv =ipGestor['MEDI'],
    elements=medi6900,
    commands=cmd6900,
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

[_get_data_amba6910,_get_data_medi6910 >> _get_data_medi6900] >> _parse_data >>  _resultados