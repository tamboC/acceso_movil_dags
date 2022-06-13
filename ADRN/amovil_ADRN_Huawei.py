"""
Este DAG corre diariamente y guarda un log con los cambios encontrados en la base de 
ADRN, respecto a la corrida anterior. 
El log se guarda en /io/cel_amovil/tmp/logs_ADRN/
También inyecta en una base influx la cantidad de parámetros relevados 
por tecnología, vendor y template.

"""
import copy
from datetime import datetime, timedelta
from lxml import etree
import os
import requests
import sys
from zipfile import ZipFile
import time

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.L_teco_db import insert_influxdb
from lib.teco_data_management import *
from lib.teco_mongodb import MongoManager

sys.path.insert(0, '/usr/local/tambo/cels/cel_amovil/airflow2/dags/lib_amovil/')
from adrn_conversion import get_ing_params


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
dag_id= DAG_ID, 
    schedule_interval = None,
    catchup = False,
    tags = ['ADRN'],
    default_args=default_args
)

path = '/io/cel_amovil/tmp/XMLs_Huawei'

mongo = MongoManager('amovil_adrn_huawei')

timestamp = datetime.now().strftime('%m/%d/%Y %H:%M:%S')

ing_params = get_ing_params('H', '4G', ["Default"])

def xml_parser(file, models, server):
    start = datetime.now()
    site_counter = 0
    checked_params = 0
    failed_params = 0
    site_params = {'server': server}
    obj = ''
    data = {}
    param = target = value = status = cell = ''
    # check = False
    check = True

    for event, element in etree.iterparse(file, events=('start',)):
        if check:
            if element.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type') != None:
                if obj in ing_params:
                    for parameter, values in data[obj].items():
                        if values:
                            if ing_params[obj][parameter]['value_type'] == 'Bit Field Type':
                                for additionalParam, value in values.items():
                                    if additionalParam != 'value_type' and value.get('value') != None:
                                        if cell and ((int(cell) < 200 and ing_params[obj][parameter][additionalParam]['element'] == 'Cell') or (int(cell) > 200 and ing_params[obj][parameter][additionalParam]['element'] == 'Cell_NbIoT')):
                                            if not site_params[obj].get(parameter):
                                                site_params[obj][parameter] = {'value_type': ing_params[obj][parameter]['value_type']}
                                            if not site_params[obj][parameter].get(additionalParam):
                                                site_params[obj][parameter][additionalParam] = copy.deepcopy(ing_params[obj][parameter][additionalParam])
                                            site_params[obj][parameter][additionalParam][f'Cell_ID {cell}'] = {'value': value['value']}
                                            site_params[obj][parameter][additionalParam][f'Cell_ID {cell}']['status'] = value['status']
                                            failed_params += 1
                                        elif ing_params[obj][parameter][additionalParam]['element'] == 'eNodeB':
                                            if not site_params[obj].get(parameter):
                                                site_params[obj][parameter] = {'value_type': ing_params[obj][parameter]['value_type']}
                                            if not site_params[obj][parameter].get(additionalParam):
                                                site_params[obj][parameter][additionalParam] = copy.deepcopy(ing_params[obj][parameter][additionalParam])
                                            site_params[obj][parameter][additionalParam]['value'] = value['value']
                                            site_params[obj][parameter][additionalParam]['status'] = value['status']
                                            failed_params += 1
                            else:
                                if cell and ((int(cell) < 200 and ing_params[obj][parameter]['element'] == 'Cell') or (int(cell) > 200 and ing_params[obj][parameter]['element'] == 'Cell_NbIoT')):
                                    if site_params[obj].get(parameter) == None:
                                        site_params[obj][parameter] = copy.deepcopy(ing_params[obj][parameter])
                                    site_params[obj][parameter][f'Cell_ID {cell}'] = {'value': values['value']}
                                    site_params[obj][parameter][f'Cell_ID {cell}']['status'] = values['status']
                                    failed_params += 1
                                elif ing_params[obj][parameter]['element'] == 'eNodeB':
                                    if site_params[obj].get(parameter) == None:
                                        site_params[obj][parameter] = copy.deepcopy(ing_params[obj][parameter])
                                    site_params[obj][parameter]['value'] = values['value']
                                    site_params[obj][parameter]['status'] = values['status']
                                    failed_params += 1
                    data = {}
                    if obj != element.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']:
                        if site_params[obj] == {}:
                            del site_params[obj]
                obj = element.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']
                if obj in ing_params:
                    data[obj] = {}
                    if not site_params.get(obj):
                        site_params[obj] = {}

            if obj in ing_params:
                param = element.tag[34:]
                if param in ing_params[obj]:
                    checked_params += 1
                    if ing_params[obj][param]['value_type'] == 'Bit Field Type':
                        for additionalParam, value in ing_params[obj][param].items():
                            if additionalParam != 'value_type':
                                target = list(map(int, value['target'].split('_')))
                                try:
                                    value = bin(int(element.text))[2:][-(target[0]+1)]
                                except:
                                    value = 0
                                status = int(value) == target[-1]
                                if not status:
                                    if not data[obj].get(param):
                                        data[obj][param] = {}
                                    data[obj][param][additionalParam] = {'value': value}
                                    data[obj][param][additionalParam]['status'] = status
                    else:
                        target = ing_params[obj][param]['target']
                        value = element.text
                        status = target == value
                        if status == False:
                            data[obj][param] = {'value': value, 'status': status}

            if element.tag == '{http://www.huawei.com/specs/SRAN}LOCALCELLID':
                cell = element.text

            if (element.tag == '{http://www.huawei.com/specs/SRAN}subsession' or element.tag == '{http://www.huawei.com/specs/SRAN}filefooter') and site_counter != 0:
                param = target = value = status = cell = ''
                mongo.insert_data(site_params)
                site_params = {'server': server}
        
        if element.attrib.get('neid'):
            # if element.attrib.get('neversion') in models:
            site_params['site'] = element.attrib.get('neid')
            site_params['neversion'] = element.attrib.get('neversion')
            site_counter += 1
            check = True
            # else:
            #     check = False
    os.remove(file)
    print(datetime.now()-start)
    return site_counter, checked_params, failed_params


def unzipper(directory, kwargs):
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) if directory in f]
    zfile = ZipFile(files[0], 'r')
    os.makedirs(os.path.join(path, directory))
    current_path = os.path.join(path, directory)
    setRuntime(kwargs, f'current_path_{directory}', current_path)
    for file in zfile.namelist():
        if '_LTE' in file or '_Co-MPT' in file:
            zfile.extract(file, path=current_path)

def files(path):
    allfiles = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    return allfiles

############################################################################################################################################################

def unzipper_amba(**kwargs):
    unzipper('AMBA', kwargs)

def unzipper_medi(**kwargs):
    unzipper('MEDI', kwargs)


def amba(**kwargs):
    site_counter = 0
    checked_params = 0
    failed_params = 0
    current_path = getRuntime(kwargs, 'current_path_AMBA')
    allfiles = files(current_path)
    for file in allfiles:
        current_site_counter, current_checked_params, current_failed_params = xml_parser(file, [], '10.75.98.19') # TODO add models
        site_counter += current_site_counter
        checked_params += current_checked_params
        failed_params += current_failed_params
    setRuntime(kwargs, 'amba_site_counter', site_counter)
    setRuntime(kwargs, 'amba_checked_params', checked_params)
    setRuntime(kwargs, 'amba_failed_params', failed_params)


def medi(**kwargs):
    site_counter = 0
    checked_params = 0
    failed_params = 0
    current_path = getRuntime(kwargs, 'current_path_MEDI')
    allfiles = files(current_path)
    for file in allfiles:
        current_site_counter, current_checked_params, current_failed_params = xml_parser(file, [], '10.75.98.89') # TODO add models
        site_counter += current_site_counter
        checked_params += current_checked_params
        failed_params += current_failed_params
    setRuntime(kwargs, 'medi_site_counter', site_counter)
    setRuntime(kwargs, 'medi_checked_params', checked_params)
    setRuntime(kwargs, 'medi_failed_params', failed_params)


def influx(**kwargs):
    amba_site_counter = getRuntime(kwargs, 'amba_site_counter')
    amba_checked_params = getRuntime(kwargs, 'amba_checked_params')
    amba_failed_params = getRuntime(kwargs, 'amba_failed_params')
    medi_site_counter = getRuntime(kwargs, 'medi_site_counter')
    medi_checked_params = getRuntime(kwargs, 'medi_checked_params')
    medi_failed_params = getRuntime(kwargs, 'medi_failed_params')

    site_counter = amba_site_counter + medi_site_counter
    checked_params = amba_checked_params + medi_checked_params
    failed_params = amba_failed_params + medi_failed_params

    print(site_counter)
    print(checked_params)
    print(failed_params)

    # data = []
    # data.append({
    #             'measurement': 'ADRN',
    #             'tags': 'Huawei_LTE',
    #             'time': timestamp,
    #             'fields':{
    #                 'sitios_analizados': site_counter,
    #                 'parametros_analizados': checked_params,
    #                 'desvios': failed_params
    #             }
    #         })
    # insert_influxdb(data, 'amovil')

def delete_files(**kwargs):
    os.rmdir(os.path.join(path, 'AMBA'))
    os.rmdir(os.path.join(path, 'MEDI'))

_unzipper_amba = PythonOperator(
    task_id='unzipper_amba',
    python_callable=unzipper_amba,
    dag=dag
)

_unzipper_medi = PythonOperator(
    task_id='unzipper_medi',
    python_callable=unzipper_medi,
    dag=dag
)

_amba = PythonOperator(
    task_id='amba',
    python_callable=amba,
    dag=dag
)

_medi = PythonOperator(
    task_id='medi',
    python_callable=medi,
    dag=dag
)

_influx = PythonOperator(
    task_id='influx',
    python_callable=influx,
    dag=dag
)

_delete_files = PythonOperator(
    task_id='delete_files',
    python_callable=delete_files,
    dag=dag
)


# _files
_unzipper_amba >> _amba >> [_influx, _delete_files]
_unzipper_medi >> _medi >> [_influx, _delete_files]