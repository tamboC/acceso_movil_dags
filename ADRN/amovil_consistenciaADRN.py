"""
DAG que busca inconsistencias en la base de ADRN
"""
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from lib.teco_data_management import *
from datetime import datetime, timedelta
from lib.L_teco_db import insert_influxdb
from lib.tambo import *
import requests
from airflow.operators.email_operator import EmailOperator
from os import remove
from lib.teco_ingeniera import Ingeniera
from lib.teco_data_management import push_data
from lib.teco_mongodb import MongoManager
import pandas as pd

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
    'provide_context': True,
    'dag_type': 'custom'
}
        
#dag
dag = DAG(
dag_id= 'amovil_consistenciaADRN', 
    schedule_interval = None,
    catchup = False,
    tags = ['ADRN'],
    default_args=default_args
)


#################################### 2) VARIABLES  ####################################

#-----------------------------------------------------------------
### Acá se deben definir los imputs para el query de parámetros en IGA
categoria_q = "ADRN"
tecnologia_q = ["4G"]
vendor_q = ["H"]
#group_q = f"{categoria_q}_M_{vendor_q}_{tecnologia_q}_PARAMS"
#tableName_q = f"{categoria_q}_M_{vendor_q}_{tecnologia_q}_PARAMS"
template_q = ["Default"]
url = "http://10.24.1.150/ingenier@/symfony/public/index.php/api/secure/shared-database/select-data"
#-----------------------------------------------------------------

#################################### 3) FUNCTIONS  ####################################

# Función para conusultar (por vendor, tecnología y template) el ADRN en IGA  
def ADRN_param(url, vendor, tech, template):

    grupo = f"{categoria_q}_M_{vendor}_{tech}_PARAMS"
    tabla = f"{categoria_q}_M_{vendor}_{tech}_PARAMS"

    consulta_json = {
                    "category": categoria_q,
                    "group": grupo,
                    "tableName": tabla ,
                    "filterCriteria": [
                        {
                            "fieldName": "template", 
                            "conditionType": "in",
                            "values": template
                        }
                    ],
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
    return requests.post(url, json=consulta_json).json()

def mmlDiscover(campoComandos):
    
    listaComandos = {"MOD": "NA",
                     "LST": "NA",
                     "DSP": "NA",
                     "ADD": "NA",
                     "RMV": "NA",}
    
    for comando in campoComandos:
        if comando.strip()[:3:] == "MOD":
            listaComandos["MOD"] = comando.strip()
        elif comando.strip()[:3:] == "LST":
            listaComandos["LST"] = comando.strip()
        elif comando.strip()[:3:] == "DSP":
            listaComandos["DSP"] = comando.strip()
        elif comando.strip()[:3:] == "ADD":
            listaComandos["ADD"] = comando.strip()
        elif comando.strip()[:3:] == "RMV":
            listaComandos["RMV"] = comando.strip()
    
    return listaComandos


def selectArchivo(vend, tech):

    path = "/io/cel_amovil/per/ADRN/"

    if vend == "H":
        if tech == "4G":
            return f"{path}paralist_ratL_en.xls"
        elif tech == "5G":
            return f"{path}paralist_ratN_en.xls"
        else:
            return "NA"
    elif vend =="N":
        return "NA"
    else:
        return "error"

def consistencia_ADRN(**kwargs):
    #logFileName = 
    
    fechaLog = (datetime.now() + timedelta(hours = -3)).strftime('%Y-%m-%d %H:%M:%S')
    logFileName = f"/io/cel_amovil/tmp/adrnConsCheck_{fechaLog}.txt"
    print(logFileName)

    for vendor in vendor_q:
        for tecnologia in tecnologia_q:

            #archivo = "/io/cel_amovil/per/ADRN/paralist_ratL_en.xls" #Template bajado de Hedex
            archivo = selectArchivo(vendor, tecnologia)
            df = pd.read_excel(archivo, sheet_name='Parameter List')    #Dataframe del template

            #respuesta = Ingeniera.get_adrn(vendor, tecnologia, templateq)   # reemplazar y no usar la funcion ADRN_param()
            respuesta = ADRN_param(url, vendor, tecnologia, template_q)
            respuesta_data = respuesta["data"]
            
            parametros = 0
            parametrosErrados = 0

            print(f"Comenzando análisis de coherencia de {categoria_q}_M_{vendor}_{tecnologia}_PARAMS, template {template_q}")
            for algo in respuesta_data:
                parametros += 1
                paramMo = algo["object"]
                paramName = algo["paramName"]
                paramVal = algo["paramValue"]
                #print(f"MO: {paramMo}")
                #print(f"Parameter ID: {paramName}")
                if "additionalParam" in algo.keys():
                    paramAdicVal = algo["additionalParam"]
                    #print(f"Value (adic): {paramAdicVal}")
                if "condition" in algo.keys():
                    paramCondic = algo["condition"]
                    #print(f"Condition: {paramCondic}")

                #print(f"Value: {paramVal}")
                if df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["Value Type"].empty:
                    print(f"{paramMo}-{paramName}: no existe")
                    parametrosErrados += 1
                    algo.update({"value_type": "error", "gui_value_range": "error", "enumeration": "error", "internal_value": "error", "comando_MOD": "error", "comando_LST": "error", "comando_DSP": "error", "comando_ADD": "error", "comando_RMV": "error"})
                else:
                    paramType = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["Value Type"].values[0]
                    #print(f"Value Type: {paramType}")
                    paramRange = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["GUI Value Range"].values[0]
                    #print(f"GUI Value Range: {paramRange}")
                    paramEnum = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["Enumeration Number/Bit"].values[0]
                    #print(f"Enumeration Number/Bit: {paramEnum}")
                    paramMml = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["MML Command"].values[0]
                    paramMml = eval("['" + paramMml[:-1].replace("\n","','") + "']")
                    paramMml_d = mmlDiscover(paramMml)

                    if paramType == "Enumeration Type" or paramType == "Bit Field Type":
                        paramEnum_d = eval("{'" +paramEnum.replace("~","':'").replace(",","','").replace(" ","") + "'}")
                        if paramType == "Enumeration Type":
                            #print(f"Internal Value: {paramEnum_d[paramVal]}")
                            if paramVal in paramEnum_d:
                                enumValue = paramEnum_d[paramVal]
                            else:
                                print(f"{paramMo}-{paramName}: no existe el valor {paramVal}")
                                parametrosErrados += 1
                                enumValue = "error"
                            #print(paramMml_d)
                            algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": enumValue, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"]})
                        elif paramType == "Bit Field Type":
                            bitMasSignificativo = max(map(int, paramEnum_d.values()))
                            if paramAdicVal in paramEnum_d:
                                bitPosicion = paramEnum_d[paramAdicVal]
                            else:
                                print(f"{paramMo}-{paramName}: no existe el bit {paramAdicVal}")
                                parametrosErrados += 1
                                bitPosicion = "error"
                            bitValue = f"{bitPosicion}_{bitMasSignificativo}_{paramVal}"
                            #print(f"Bit: {bitPosicion} de {bitMasSignificativo} (Value: {bitValue})")
                            algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": bitValue, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"]})
                    else:
                        #print(f"Internal Value: {paramVal}")
                        algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": paramVal, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"]})
                    #print("------------------------------------------------------")
                
            print(f"Fin del análisis. Se verificaron {parametros} parámetros, encontrando {parametrosErrados} errores")
                    
    #return respuesta_data

###################################### 1) TASKS  ######################################

consistencia_ADRN = PythonOperator(
    task_id ='consistencia_ADRN',
    python_callable = consistencia_ADRN,
    dag=dag
)

#################################### 4) WORKFLOW  #####################################

consistencia_ADRN