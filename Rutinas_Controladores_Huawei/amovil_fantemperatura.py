"""
Pasos de hacer un DAG

1. TASKS
2. VARIABLES
3. FUNCTIONS
4. WORKFLOW

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
    #'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}
        
#dag
dag = DAG(
dag_id= 'amovil_FANTEMPERATURE', 
    schedule_interval= '@hourly', 
    #schedule_interval= None, 
    catchup = False,
    tags = ['Controladores', 'Huawei'],
    default_args=default_args
)


#################################### 2) VARIABLES  ####################################

amba6910 = ['USHRNC02',
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
            'BSCUSH4',
            'BSCMT10',
            'BSCMUN1',
            'BSCBEL1',
            'BSCBHB4',
            'BSCCLI1',
            'BSCBCH4',
            'BSCANA3',
            'BSCMDP4',
            ]

medi6900 =['LRJRNC02',
            'CTMRNC02',
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
            ]

medi6910 =['MDZRNC04',
            'CORRNC04',
            'TUCRNC03',
            'RAFRNC01',
            'SNTRNC02',
            'MDZRNC05',
            'SLTRNC03',
            'TUCRNC04',
            'CORRNC05',
            'BSCMDZ4',
            'BSCRAF1',
            ]

cmd6900 = ['DSP FANTEMPERATURE:FANFLAG=NORMAL_FAN,SRN=0;',
            'DSP FANTEMPERATURE:FANFLAG=NORMAL_FAN,SRN=1;',
            'DSP FANTEMPERATURE:FANFLAG=NORMAL_FAN,SRN=2;',
            'DSP FANTEMPERATURE:FANFLAG=NORMAL_FAN,SRN=3;',
            'DSP FANTEMPERATURE:FANFLAG=NORMAL_FAN,SRN=4;',
            'DSP FANTEMPERATURE:FANFLAG=NORMAL_FAN,SRN=5;',]

cmd6910 = ['DSP FANTEMPERATURE:;']

subrack = ""

temperatura = ""

fanid = ""

#################################### 3) FUNCTIONS  ####################################

def parse_data(**kwargs):
#    print(today)
    output_amba = pull_data(kwargs,'get_data_amba6910')[0]
    output_medi = pull_data(kwargs,'get_data_medi6910')[0]
    output_amba.update(output_medi)
    output_medi = pull_data(kwargs,'get_data_medi6900')[0]
    output_amba.update(output_medi)
    
    timestamp = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    data = []

    for key,value in output_amba.items():
        for linea in value:
            if linea[1:3].strip().isdigit():
                subrack = linea[1:3].strip()
                if linea.find('Fan ')>0:
                    fanid = linea[14:22].strip()
                    temperatura = linea[24:26].strip()
                if linea.find('Normal fan ')>0:
                    fanid = linea[14:26].strip()
                    temperatura = linea[26:31].strip() 
                print(key + ', Subrack: ' + subrack + ', Fan ID: ' + fanid + ', Temperatura: ' + temperatura)
                data.append({
                    'measurement': 'FanTemperaturaRnc',
                    'tags': {
                            'central_name':key,
                            'subrack': subrack,
                            'fanid': fanid
                            },
                    'time': timestamp,
                    'fields':{
                        'tempInt': int(temperatura)
                    }
                })
            #else:
            #    print('No hay información de Temperatura')
    insert_influxdb(data, 'amovil')
    #print(data)

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
    srv ='10.75.98.19',     #SLO
    #srv = '10.75.102.5',   #HOR
    elements=amba6910,
    commands=cmd6910,
    dag=dag
)

_get_data_medi6910 = TecoMaeOperator(
    task_id = 'get_data_medi6910',
    srv ='10.75.98.89',       #SLO
    #srv ='10.75.102.26',     #HOR
    elements=medi6910,
    commands=cmd6910,
    dag=dag
)

_get_data_medi6900 = TecoMaeOperator(
    task_id = 'get_data_medi6900',
    srv ='10.75.98.89',
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

[_get_data_medi6910 >> _get_data_medi6900, _get_data_amba6910] >> _parse_data >>  _resultados