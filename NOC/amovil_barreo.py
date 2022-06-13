"""
DAG para barrear y desbarrear celdas

"""
####################################### LIBRERIAS #######################################
import os
import re
import time
################################### LIBRERIAS AIRFLOW ###################################
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
################################### LIBRERIAS LOCALES ###################################
from lib.teco_data_management import *
from lib.teco_huawei_conn import HuaweiConnector
from lib.teco_mongodb import MongoManager
from lib.tambo import TAMBO
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
    schedule_interval= None,
    default_args=default_args
)

#############################################################################################################################################################
###################################################################         MONGO       #####################################################################
mongo = MongoManager('amovil_cambio_celdas')
def data_to_mongo(user_id, site, cell, rnc, action):
    data = {
        "legajo": user_id,
        "sitio": site,
        "celda": cell,
        "rnc": rnc,
        "accion": action,
        "fecha": str(datetime.now() - timedelta(hours=3))
    }
    mongo.insert_data(data)

#############################################################################################################################################################
###################################################################         UARFCN        ###################################################################
def get_uarfcn(huawei, site, freqs):
    output = huawei.exec_commands(site, ['LST UTRANNFREQ:;'])
    uarfcn_set = set()
    for line in output['LST UTRANNFREQ:;']:
        if line[:3].strip().isdigit():
            if line[12:25].strip() in freqs[0] or line[12:25].strip() in freqs[1]:
                uarfcn_set.add(line[12:25].strip())
    if len(uarfcn_set) == 1:
        uarfcn = uarfcn1 = uarfcn_set.pop()
    if len(uarfcn_set) == 2:
        uarfcn, uarfcn1 = uarfcn_set
    return uarfcn, uarfcn1

def get_uarfcn_desbarreo(huawei, site, cell, server):
    if cell[-3:-2] == 'W':
        uarfcn='3062'
        uarfcn1=uarfcn
    elif cell[-3:-1] == 'V1' and server == '10.75.98.19':
        uarfcn='4382'
        uarfcn1=uarfcn
    elif cell[-3:-1] == 'V1' and server == '10.75.98.89':
        uarfcn, uarfcn1 = get_uarfcn(huawei, site, ['4358', '4363'])
    elif cell[-3:-1]=='V2' and server == '10.75.98.19':
        uarfcn='4358'
        uarfcn1=uarfcn
    elif cell[-3:-1] == 'V2' and server == '10.75.98.89':
        uarfcn, uarfcn1 = get_uarfcn(huawei, site, ['4384', '4379'])
    elif cell[-3:-1] == 'U1' and server == '10.75.98.19':
        uarfcn, uarfcn1 = get_uarfcn(huawei, site, ['9788', '9912'])
    elif cell[-3:-1] == 'U1' and server == '10.75.98.89':
        uarfcn='9813'
        uarfcn1=uarfcn
    elif cell[-3:-1] == 'U2' and server == '10.75.98.19':
        uarfcn, uarfcn1 = get_uarfcn(huawei, site, ['9813', '9937'])
    elif cell[-3:-1] == 'U2' and server == '10.75.98.89':
        uarfcn='9834'
        uarfcn1=uarfcn
    elif cell[-3:-1] == 'U3' and server == '10.75.98.19':
        uarfcn='9889'
        uarfcn1=uarfcn
    elif cell[-3:-1] == 'U4' and server == '10.75.98.19':
        uarfcn='9868'
        uarfcn1=uarfcn
    return uarfcn, uarfcn1


def get_uarfcn_barreo(cell, server):
    if cell[-3:-2]=='W':
        uarfcn=3062
        uarfcn1=3062
    elif cell[-3:-1]=='V1' and server=='10.75.98.19':
        uarfcn=4382
        uarfcn1=4382
    elif cell[-3:-1]=='V1' and server=='10.75.98.89':
        uarfcn=4358
        uarfcn1=4363
    elif cell[-3:-1]=='V2' and server=='10.75.98.19':
        uarfcn=4358
        uarfcn1=4358
    elif cell[-3:-1]=='V2' and server=='10.75.98.89':
        uarfcn=4384
        uarfcn1=4379
    elif cell[-3:-1]=='U1' and server=='10.75.98.19':
        uarfcn=9788
        uarfcn1=9912
    elif cell[-3:-1]=='U1' and server=='10.75.98.89':
        uarfcn=9813
        uarfcn1=9813
    elif cell[-3:-1]=='U2' and server=='10.75.98.19':
        uarfcn=9813
        uarfcn1=9937
    elif cell[-3:-1]=='U2' and server=='10.75.98.89':
        uarfcn=9834
        uarfcn1=9834
    elif cell[-3:-1]=='U3' and server=='10.75.98.19':
        uarfcn=9889
        uarfcn1=9889
    elif cell[-3:-1]=='U4' and server=='10.75.98.19':
        uarfcn=9868
        uarfcn1=9868
    return uarfcn, uarfcn1

#############################################################################################################################################################
########################################################################     3G     #########################################################################
def action_3G(huawei, user_id, server, site, cells, rnc, action):
    lte_cells = huawei.get_lte_cells_conf(site)
    cell_data = huawei.get_wcdma_cells_conf(rnc, site)
    result = ''
    flag = False
    for cell in cells:
        if cell[-1]== "1" or cell[-1]== "4":
            r = 0
        elif cell[-1]== "2" or cell[-1]== "5":
            r = 1
        elif cell[-1]== "3" or cell[-1]== "6":
            r = 2
        for c in cell_data:
            if c['cell_name'] == cell:
                cell_id = c['cell_id']
                break
        if action == 'barreo':
            uarfcn, uarfcn1 = get_uarfcn_barreo(cell, server)
            output = huawei.exec_commands(rnc, [f'MOD UCELLACCESSSTRICT:CELLID={cell_id},IDLECELLBARRED=BARRED,IDLEINTRAFREQRESELECTION=ALLOWED,IDLETBARRED=D320,CONNCELLBARRED=BARRED,CONNINTRAFREQRESELECTION=NOT_ALLOWED,CONNTBARRED=D1280;', 
                                                f'LST UCELLACCESSSTRICT:CELLID={cell_id};'])
            for line in output[f'LST UCELLACCESSSTRICT:CELLID={cell_id};']:
                if 'Cell barred indicator for SIB3  =  BARRED' in line:
                    result += f"La celda {c['cell_name']}({c['cell_id']}) se encuentra Barreada.\n"
                    data_to_mongo(user_id, site, c['cell_name'], rnc, action)
                    flag = True
                if 'Cell barred indicator for SIB3  =  NOT_BARRED' in line:
                    result += f"La celda {c['cell_name']}({c['cell_id']}) no se Barro. Vuelva a intentarlo.\n"
            if flag:
                commands_block = [f'BLK CELL:LOCALCELLID={lte_cell["cell_id"]},CELLADMINSTATE=CELL_HIGH_BLOCK;' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r]
                commands_utranncell = [f'RMV UTRANNCELL:LOCALCELLID={lte_cell["cell_id"]};' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r]
                commands_utranranshare = [f'RMV UTRANRANSHARE:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn},MCC="722",MNC="34";' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r]
                commands_utrannfreq = [f'RMV UTRANNFREQ:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn};' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r]
                commands_utranranshare_extra = []
                commands_utrannfreq_extra = []
                if uarfcn != uarfcn1:
                    commands_utranranshare_extra = [f'RMV UTRANRANSHARE:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn1},MCC="722",MNC="34";' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r]
                    commands_utrannfreq_extra = [f'RMV UTRANNFREQ:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn1};' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r]
                commands_unblock = [f'UBL CELL:LOCALCELLID={lte_cell["cell_id"]};' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r]
                commands = commands_block + commands_utranncell + commands_utranranshare + commands_utranranshare_extra + commands_utrannfreq + commands_utrannfreq_extra + commands_unblock
                huawei.exec_commands(site, commands)
        else:
            output = huawei.exec_commands(rnc, [f'MOD UCELLACCESSSTRICT:CELLID={cell_id},ISACCESSCLASS0BARRED=NOT_BARRED,ISACCESSCLASS1BARRED=NOT_BARRED,ISACCESSCLASS2BARRED=NOT_BARRED,ISACCESSCLASS3BARRED=NOT_BARRED,ISACCESSCLASS4BARRED=NOT_BARRED,ISACCESSCLASS5BARRED=NOT_BARRED,ISACCESSCLASS6BARRED=NOT_BARRED,ISACCESSCLASS7BARRED=NOT_BARRED,ISACCESSCLASS8BARRED=NOT_BARRED,ISACCESSCLASS9BARRED=NOT_BARRED,ISACCESSCLASS10BARRED=NOT_BARRED,ISACCESSCLASS11BARRED=NOT_BARRED,ISACCESSCLASS12BARRED=NOT_BARRED,ISACCESSCLASS13BARRED=NOT_BARRED,ISACCESSCLASS14BARRED=NOT_BARRED,ISACCESSCLASS15BARRED=NOT_BARRED,IDLECELLBARRED=NOT_BARRED,CONNCELLBARRED=NOT_BARRED;', 
                                                f'LST UCELLACCESSSTRICT:CELLID={cell_id};'])
            for line in output[f'LST UCELLACCESSSTRICT:CELLID={cell_id};']:
                if 'Cell barred indicator for SIB3  =  NOT_BARRED' in line:
                    result += f"La celda {c['cell_name']}({c['cell_id']}) se Desbarreo.\n"
                    data_to_mongo(user_id, site, c['cell_name'], rnc, action)
                    flag = True
                if 'Cell barred indicator for SIB3  =  BARRED' in line:
                    result += f"La celda {c['cell_name']}({c['cell_id']}) no se Desbarreo. Vuelva a intentarlo.\n"
            if flag:
                uarfcn, uarfcn1 = get_uarfcn_desbarreo(huawei, site, cell, server)
                commands_utrannfreq = [f'ADD UTRANNFREQ:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn},UTRANFDDTDDTYPE=UTRAN_FDD,UTRANULARFCNCFGIND=NOT_CFG,CELLRESELPRIORITYCFGIND=CFG,CELLRESELPRIORITY=3;' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r and int(lte_cell['cell_id']) < 200]
                commands_utranranshare = [f'ADD UTRANRANSHARE:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn},MCC="722",MNC="34",CELLRESELPRIORITYCFGIND=NOT_CFG;' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r and int(lte_cell['cell_id']) < 200]
                commands_utrannfreq_extra = []
                commands_utranranshare_extra = []
                if uarfcn != uarfcn1:
                    commands_utrannfreq_extra = [f'ADD UTRANNFREQ:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn1},UTRANFDDTDDTYPE=UTRAN_FDD,UTRANULARFCNCFGIND=NOT_CFG,CELLRESELPRIORITYCFGIND=CFG,CELLRESELPRIORITY=3;' for lte_cell in lte_cells if int(lte_cell) % 3 == r and int(lte_cell['cell_id']) < 200]
                    commands_utranranshare_extra = [f'ADD UTRANRANSHARE:LOCALCELLID={lte_cell["cell_id"]},UTRANDLARFCN={uarfcn1},MCC="722",MNC="34",CELLRESELPRIORITYCFGIND=NOT_CFG;' for lte_cell in lte_cells if int(lte_cell['cell_id']) % 3 == r and int(lte_cell['cell_id']) < 200]
                commands = commands_utrannfreq + commands_utrannfreq_extra + commands_utranranshare + commands_utranranshare_extra
                huawei.exec_commands(site, commands)
    return result

#############################################################################################################################################################
########################################################################     4G     #########################################################################
def action_4G(huawei, user_id, site, cells, action):
    result = ''
    cell_data = huawei.get_lte_cells_conf(site)
    for cell in cells:
        for c in cell_data:
            if c['cell_name'] == cell:
                cell_id = c['cell_id']
                break
        cell_barred = 'CELL_NOT_BARRED;'
        if action == 'barreo':
            cell_barred = 'CELL_BARRED;'
        output = huawei.exec_commands(site, [f'MOD CELLACCESS:LOCALCELLID={cell_id},CELLBARRED={cell_barred};'])
        if output[f'MOD CELLACCESS:LOCALCELLID={cell_id},CELLBARRED={cell_barred};'][-1] == "RETCODE = 0  '":
            if action == 'barreo':
                result += f"La celda {c['cell_name']}({c['cell_id']}) se encuentra Barreada.\n"
                data_to_mongo(user_id, site, c['cell_name'], '', action)
            else:
                result += f"La celda {c['cell_name']}({c['cell_id']}) se encuentra Desbarreada.\n"
                data_to_mongo(user_id, site, c['cell_name'], '', action)
        else:
            if action == 'barreo':
                result += f"La celda {c['cell_name']}({c['cell_id']}) no se Barreo. Vuelva a intentarlo.\n"
            else:
                result += f"La celda {c['cell_name']}({c['cell_id']}) no se Desbarreo. Vuelva a intentarlo.\n"
    return result

#############################################################################################################################################################
###############################################################      FUNCION PRINCIPAL      #################################################################
def execution(**kwargs):
    user_id = kwargs["dag_run"].conf.get('_user_id')
    action = kwargs["dag_run"].conf.get('_action')
    site = kwargs["dag_run"].conf.get('_site').upper()
    cells = kwargs["dag_run"].conf.get('_cells').upper().split(",")
    rnc = kwargs["dag_run"].conf.get('_rnc')
    mongo_sitios = MongoManager('amovil_sitios')
    query = mongo_sitios.make_query({'Equipo':site})
    if not query:
        return f'El sitio ingresado ({site}) no se encuentra en la base de datos.\nVerificá el formulario y volvé a ejecutar la tarea o contactate con el administrador.'
    server = query.get('Server')
    mongo_sitios.close_connection()
    huawei = HuaweiConnector(server)
    if rnc == '':
        result = action_4G(huawei, user_id, site, cells, action)
    else:
        for cell in cells:
            if cell[-3] not in ['U', 'V', 'W']:
                return 'Seleccionaste una RNC pero las celdas corresponden a otra tecnología.\nVerificá el formulario y volvé a ejecutar la tarea.'
        result = action_3G(huawei, user_id, server, site, cells, rnc, action)
    huawei.disconnection()
    mongo.close_connection()
    return result




_execution = PythonOperator(
    task_id='execution',
    python_callable=execution,
    dag=dag
)

_execution