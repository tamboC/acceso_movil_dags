"""
    Documentar codigo con Docstring
    Path de este directorio /usr/local/airflow/dags/cel_amovil
    Path de la carpeta Ansible /urs/local/ansible/
"""

'''
Dag amovil
'''

"""
Pasos de hacer un DAG

1. TASKS
2. VARIABLES
3. FUNCTIONS
4. WORKFLOW

"""
######################## LIBRERIAS #####################################

import pandas as pd
import requests
import os, shutil,zipfile
from zipfile import ZipFile

######################## LIBRERIAS AIRFLOW #############################

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator

######################## LIBRERIAS LOCALES #############################

from lib.teco_data_management import *
from teco_mae_operator.operators.tecoMaeOperator import TecoMaeOperator
from lib.tambo import *

############# Importación de librerias de amovil #######################
import sys
sys.path.insert(0, '/usr/local/tambo/cels/cel_amovil/airflow2/dags/lib_amovil/')
from sendEmail import send_email
from limpieza_temp import LimpiezaTemp
#######################################################################

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
#    'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}
        

#dag
dag = DAG(
    dag_id='amovil_ReporteADRN', 
    schedule_interval= None,
    catchup = False,
    description='Reporte ADRN',
    tags = ['ADRN', 'Reporte'],
    default_args=default_args
)

#################################### 2) VARIABLES  ####################################

io_path_log = "/io/cel_amovil/tmp/Reporte_ADRN/"

def GetDataFunction(**kwargs):

    datos  =  TAMBO.GetForm(kwargs) # Obtenemos un dict con la data
    #datos  =  TAMBO.GetForm('amovil_ReporteADRN')
    #datos   =  TAMBO.GetForms('amovil_ReporteADRN') # Obtenemos un dict con la data
    
    # email   =  datos["email"]
    vendor  =  datos["selectBoxes"]
    tech    =  datos["tech"]
    vendors =  []
    techs   =  []

    for k,v in vendor.items():
        if v == True:
            vendors.append(k)

    for k,v in tech.items():
        if v == True:
            techs.append(k)

    
    args = []
    
    for vendor1 in vendors:
        for tech1 in techs:
            
            args.append(vendor1 + tech1)
        
    ####### Contructor del reporte de ADRN #######

    url = "http://10.24.1.150/ingenier@/symfony/public/index.php/api/secure/shared-database/select-data" # API de IGA

    payload, api_to_json, json_to_dataframe = [], [], []

    for arg in args:
        
        myjson = {"category": "ADRN",
                "group": f'ADRN_M_{arg}_PARAMS',
                "tableName": f'ADRN_M_{arg}_PARAMS',
                "filterCriteria": [],
                "orderCriteria": [
                    {
                        "fieldName": "object",
                        "order": "asc"
                    },
                    {
                        "fieldName": "paramName",
                        "order": "asc"
                    }
                ],
                "resolveCustomChoiceValues": True
                }
        api_to_json = requests.post(url, json=myjson).json()
        
        json_to_dataframe = pd.DataFrame(api_to_json["data"], columns=["id", "element", "object", "paramName",
                                                                    "additionalParam", "condition", "paramValue", "objectInfo", "conditionExtension1", "conditionExtension2",
                                                                    "version", "date", "comment", "action", "modification", "template"])
        # Filtramos en la columna Template lo que diga Default, por si hay alguna carga temporal de parametros                                                            
        json_to_dataframe = json_to_dataframe[json_to_dataframe.template == 'Default']

        if arg[0] == 'H':
            json_to_dataframe = json_to_dataframe.drop(
                ['id', 'date', 'action', 'modification', 'template'], axis=1)
            # Reordenamos las columnas
            column_names = ['Elemento', 'MOC', 'Parameter', 'Parametro Adicional', 'Condition', 'Value',
                            'Object Info', 'Extension 1<Condition>', 'Extension 2<Condition>', 'Extension 2<Description>', 'Extension 1<Description>']
            json_to_dataframe.columns = column_names
            json_to_dataframe = json_to_dataframe[['Elemento', 'MOC', 'Parameter', 'Parametro Adicional', 'Condition', 'Value',
                                                   'Object Info', 'Extension 1<Condition>', 'Extension 2<Condition>', 'Extension 1<Description>', 'Extension 2<Description>']]
            json_to_dataframe = json_to_dataframe.fillna('')
            json_to_dataframe['Value'] = (json_to_dataframe['Parametro Adicional'] + '-' +
                                          json_to_dataframe['Value']).where(json_to_dataframe['Parametro Adicional'] != '',
                                                                            json_to_dataframe['Value'])
            json_to_dataframe = json_to_dataframe.drop(
                ['Parametro Adicional'], axis=1)
            json_to_dataframe = json_to_dataframe[['Elemento', 'MOC', 'Parameter', 'Value', 'Condition',
                                                   'Object Info', 'Extension 1<Condition>',
                                                   'Extension 2<Condition>', 'Extension 1<Description>', 'Extension 2<Description>']]
            if arg[2:] == '4G':
                json_to_dataframe_eNodeB = json_to_dataframe[json_to_dataframe.Elemento == 'eNodeB']
                json_to_dataframe_eNodeB = json_to_dataframe_eNodeB.drop(
                    ['Elemento'], axis=1)

                json_to_dataframe_Cell = json_to_dataframe[json_to_dataframe.Elemento == 'Cell']
                json_to_dataframe_Cell = json_to_dataframe_Cell.drop(
                    ['Elemento'], axis=1)
                    
                json_to_dataframe_CellNbIoT = json_to_dataframe[json_to_dataframe.Elemento == 'Cell_NbIoT']
                json_to_dataframe_CellNbIoT = json_to_dataframe_CellNbIoT.drop(['Elemento'], axis=1)

                with pd.ExcelWriter(f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx') as writer:
                    json_to_dataframe_eNodeB.to_excel(
                        writer, sheet_name='eNodeB', index=False)
                    json_to_dataframe_Cell.to_excel(
                        writer, sheet_name='Cell', index=False)
                    json_to_dataframe_CellNbIoT.to_excel(
                        writer, sheet_name='CellNbIoT', index=False)
						
            elif arg[2:] == '3G':
                json_to_dataframe_RNC = json_to_dataframe[json_to_dataframe.Elemento == 'RNC']
                json_to_dataframe_RNC = json_to_dataframe_RNC.drop(
                    ['Elemento'], axis=1)

                json_to_dataframe_NodeB = json_to_dataframe[json_to_dataframe.Elemento == 'NodeB']
                json_to_dataframe_NodeB = json_to_dataframe_NodeB.drop(
                    ['Elemento'], axis=1)

                json_to_dataframe_Cell = json_to_dataframe[json_to_dataframe.Elemento == 'Cell']
                json_to_dataframe_Cell = json_to_dataframe_Cell.drop(
                    ['Elemento'], axis=1)

                with pd.ExcelWriter(f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx') as writer:
                    json_to_dataframe_RNC.to_excel(
                        writer, sheet_name='RNC', index=False)
                    json_to_dataframe_NodeB.to_excel(
                        writer, sheet_name='NodeB', index=False)
                    json_to_dataframe_Cell.to_excel(
                        writer, sheet_name='Cell', index=False)
            elif arg[2:] == '2G':
                json_to_dataframe_BSC = json_to_dataframe[json_to_dataframe.Elemento == 'BSC']
                json_to_dataframe_BSC = json_to_dataframe_BSC.drop(
                    ['Elemento'], axis=1)

                json_to_dataframe_Cell = json_to_dataframe[json_to_dataframe.Elemento == 'Cell']
                json_to_dataframe_Cell = json_to_dataframe_Cell.drop(
                    ['Elemento'], axis=1)

                with pd.ExcelWriter(f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx') as writer:
                    json_to_dataframe_BSC.to_excel(
                        writer, sheet_name='BSC', index=False)
                    json_to_dataframe_Cell.to_excel(
                        writer, sheet_name='Cell', index=False)
            else:
                json_to_dataframe = json_to_dataframe.drop(
                    ['Elemento'], axis=1)
                json_to_dataframe.to_excel(
                    f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx', index=False)
        else:
            json_to_dataframe = json_to_dataframe.drop(
                ['id', 'objectInfo', 'conditionExtension1', 'conditionExtension2', 'template'], axis=1)
            # Reordenamos las columnas
            column_names = ['Elemento', 'MO Class', 'Abbreviated Name', 'Parent Structure', 'Rule', 'GUI',
                            'Versión', 'Fecha', 'Comentarios', 'accion', 'Modification']
            json_to_dataframe.columns = column_names
            json_to_dataframe = json_to_dataframe[['Elemento', 'MO Class', 'Abbreviated Name', 'Parent Structure', 'GUI', 'Rule',
                                                   'Versión', 'Fecha', 'Modification', 'accion', 'Comentarios']]
            json_to_dataframe = json_to_dataframe.fillna('')

            if arg[2:] == '4G':
                json_to_dataframe_MRBTS_without_rule = json_to_dataframe[json_to_dataframe.Rule == '']
                json_to_dataframe_MRBTS_without_rule = json_to_dataframe_MRBTS_without_rule.drop(
                    ['Elemento', 'Rule'], axis=1)

                json_to_dataframe_MRBTS_with_rule = json_to_dataframe[json_to_dataframe.Rule != '']
                json_to_dataframe_MRBTS_with_rule = json_to_dataframe_MRBTS_with_rule.drop([
                                                                                           'Elemento'], axis=1)

                with pd.ExcelWriter(f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx') as writer:
                    json_to_dataframe_MRBTS_without_rule.to_excel(
                        writer, sheet_name='ADRN_4G', index=False)
                    json_to_dataframe_MRBTS_with_rule.to_excel(
                        writer, sheet_name='ADRN_4G_Adv', index=False)
            elif arg[2:] == '3G':
                json_to_dataframe_RNC_without_rule = json_to_dataframe[json_to_dataframe.Rule == '']
                json_to_dataframe_RNC_without_rule = json_to_dataframe_RNC_without_rule.drop(
                    ['Elemento', 'Rule'], axis=1)

                json_to_dataframe_RNC_with_rule = json_to_dataframe[json_to_dataframe.Rule != '']
                json_to_dataframe_RNC_with_rule = json_to_dataframe_RNC_with_rule.drop([
                                                                                       'Elemento'], axis=1)

                with pd.ExcelWriter(f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx') as writer:
                    json_to_dataframe_RNC_without_rule.to_excel(
                        writer, sheet_name='ADRN_3G', index=False)
                    json_to_dataframe_RNC_with_rule.to_excel(
                        writer, sheet_name='ADRN_3G_Adv', index=False)
            elif arg[2:] == '2G':
                json_to_dataframe_BSC_without_rule = json_to_dataframe[json_to_dataframe.Rule == '']
                json_to_dataframe_BSC_without_rule = json_to_dataframe_BSC_without_rule.drop(
                    ['Elemento', 'Rule'], axis=1)

                json_to_dataframe_BSC_with_rule = json_to_dataframe[json_to_dataframe.Rule != '']
                json_to_dataframe_BSC_with_rule = json_to_dataframe_BSC_with_rule.drop([
                                                                                       'Elemento'], axis=1)

                with pd.ExcelWriter(f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx') as writer:
                    json_to_dataframe_BSC_without_rule.to_excel(
                        writer, sheet_name='ADRN_2G', index=False)
                    json_to_dataframe_BSC_with_rule.to_excel(
                        writer, sheet_name='ADRN_2G_Adv', index=False)
            else:
                json_to_dataframe = json_to_dataframe.drop(
                    ['Elemento'], axis=1)
                json_to_dataframe.to_excel(
                    f'{io_path_log}/ADRN_Seleccion_{arg}.xlsx', index=False)
            
    
    
    folder = io_path_log
    
####### Se comprimen los reportes ######

    
    zf = zipfile.ZipFile(f"{folder}/Reporte_ADRN.zip", "w")
    for dirname, subdirs, files in os.walk(folder):
        for filename in files:
            if filename != 'Reporte_ADRN.zip':
                zf.write(os.path.join(dirname, filename), filename)
    zf.close()        

###### Constructor del email #######

def BuildMail(**kwargs):

    #datos   =  TAMBO.GetForm('amovil_ReporteADRN') # Obtenemos un dict con la data
    datos  =  TAMBO.GetForm(kwargs) # Obtenemos un dict con la data
    email   =  datos["email"]

    asunto    = "Reporte ADRN"
    texto     = """
                Se adjunta el archivo comprimido con o los reportes de ADRN que solicito.
                <br>
                <br>
                Cualquier duda mandar un mail a AMAutomation@teco.com.ar.
                <br>
                <br>
                ¡Muchas Gracias! 
                """  
    adjunto   = io_path_log + "Reporte_ADRN.zip"    
    emailPara = email
    
    send_email(emailPara, asunto, texto, adjunto)

    result = 'Se envió el mail satisfactoriamente'

    return result
    

       
###### Remueve los archivos temporales ######

def Temp_Limpieza(**kwargs):
    
    folder = io_path_log
    LimpiezaTemp(folder)
    #print("Acá se haría la limpieza del temporal")
           

###################################### 1) TASKS  ######################################

_GetDataFunction = PythonOperator(
    task_id ='GetDataFunction',
    python_callable = GetDataFunction,
    dag=dag
)

_BuildMail = PythonOperator(
    task_id ='BuildMail',
    python_callable = BuildMail,
    dag=dag
)

_Temp_Limpieza = PythonOperator(
    task_id ='Temp_Limpieza',
    python_callable = Temp_Limpieza,
    dag=dag
)

#################################### 4) WORKFLOW  #####################################

_GetDataFunction >> _BuildMail >> _Temp_Limpieza

