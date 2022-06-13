"""
Este DAG corre diariamente y guarda un log con los cambios encontrados en la base de 
ADRN, respecto a la corrida anterior. 
El log se guarda en /io/cel_amovil/tmp/logs_ADRN/
También inyecta en una base influx la cantidad de parámetros relevados 
por tecnología, vendor y template.

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
from lib.teco_data_management import push_data
from lib.teco_mongodb import MongoManager
import yaml
from lib.teco_ingeniera import Ingeniera

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
dag_id= 'amovil_logADRN', 
    schedule_interval =  '0 9 * * *',
    catchup = False,
    description='Log diario de cambios en ADRN',
    tags = ['ADRN'],
    default_args=default_args
)


#################################### 2) VARIABLES  ####################################

# IP IGA 10.24.1.150
# url = "http://ing-acc-movil01/ingenier@/symfony/public/index.php/api/secure/shared-database/select-data"
# url_select_data = "http://10.24.1.150/ingenier@/symfony/public/index.php/api/secure/shared-database/select-data"
url_history = "http://10.24.1.150/ingenier@/symfony/public/index.php/api/secure/shared-database/change-history"
categoria_q = "ADRN"
#-----------------------------------------------------------
#vendor = ["H", "N"]
#tech = ["2G", "3G", "4G", "5G"]
#template_q = ["Default", "enDespliegue"]                            # Templates para mostrar en el log de cambios
#templateList = [["Default"], ["enDespliegue"], ["3911B_eRAN15"]]    # Templates para Grafana
#io_path_log = "/io/cel_amovil/tmp/logs_ADRN/"
#-----------------------------------------------------------
ruta_Config = '/usr/local/airflow/dags/cel_amovil/Config/logADRN.yml'
medicionInflux = "cantParametrosADRN"
correccionHora = -3

#destinatarios = ["LCIuale@teco.com.ar", "luciano.iuale@gmail.com"]

#################################### 3) FUNCTIONS  ####################################
### FUNCIONES INDEPENDIENTES ###
# Función para leer el yaml de configuración
def import_yaml(archivo):
	with open(archivo, 'r') as file:
		Config = yaml.safe_load(file)

	print(f"Datos de configuración: {Config}")

	return Config

# Función para convertir el nombre corto en largo de los vendors
def vendorLongName(vendor):
    if vendor == "H":
        return "Huawei"
    elif vendor == "N":
        return "Nokia"
    else:
        return "ERROR"

# Función para conusultar el Change History del ADRN en IGA    
def ADRN_history(url,vendor,tech):
    grupo = f"ADRN_M_{vendor}_{tech}_PARAMS"
    tabla = f"ADRN_M_{vendor}_{tech}_PARAMS"
    
    consulta_json = {
                      "category": "ADRN",
                      "group": grupo,
                      "tableName": tabla,
                      "filterCriteria": [
                        # {
                        #     "fieldName": "dateTime",
                        #     "ConditionType": "contains",
                        #     "values": ["2021-05"]
                        # }
                      ],
                      "orderCriteria": [
                        {
                            "fieldName": "dateTime",
                            "order": "asc"
                        }
                      ]
                     }
    
    return requests.post(url, json=consulta_json).json()

# Función para conusultar (por vendor, tecnología y template) el ADRN en IGA  
# def ADRN_param(url, vendor, tech, template):
#     grupo = f"{categoria_q}_M_{vendor}_{tech}_PARAMS"
#     tabla = f"{categoria_q}_M_{vendor}_{tech}_PARAMS"

#     consulta_json = {
# 					"category": categoria_q,
# 					"group": grupo,
# 					"tableName": tabla ,
# 					"filterCriteria": [
# 						{
# 							"fieldName": "template", 
# 							"conditionType": "in",
# 							"values": template
# 						}
# 					],
# 					"orderCriteria": [
# 						{
# 							"fieldName": "object",
# 							"order": "asc"
# 						},
# 						{
# 							"fieldName": "paramName",
# 							"order": "asc"
# 						}
# 					],
# 					"resolveCustomChoiceValues": True
# 					}
#     return requests.post(url, json=consulta_json).json()

def correoHTML(nombreArchivo, resumenArchivo):
    textoMail = '<!DOCTYPE html><html><style>body {font-family: arial;}table, th, td {border:1px solid black; border-collapse: collapse;}</style><body>'
    textoMail = textoMail + f"Se adjunta '{nombreArchivo}'<br><br>El resumen de cambios es el siguiente:<br><br>"

    textoMail = textoMail + "<table border=1px black cellpadding=10 cellspacing=0>"
    textoMail = textoMail + "<tr>"
    textoMail = textoMail + "  <td><strong>tabla</strong></td>"
    textoMail = textoMail + "  <td><strong>altas</strong></td>"
    textoMail = textoMail + "  <td><strong>bajas</strong></td>"
    textoMail = textoMail + "  <td><strong>modificaciones</strong></td>"
    textoMail = textoMail + "</tr>"
    
    for datos in resumenArchivo:
        if datos["altas"] + datos["bajas"] + datos["modificaciones"] > 0:
            #textoMail = textoMail + str(datos) + "<br>"
            textoMail = textoMail + "<tr>"
            textoMail = textoMail + "  <td>" + str(datos["tabla"]) + "</td>"
            textoMail = textoMail + "  <td>" + str(datos["altas"]) + "</td>"
            textoMail = textoMail + "  <td>" + str(datos["bajas"]) + "</td>"
            textoMail = textoMail + "  <td>" + str(datos["modificaciones"]) + "</td>"
            textoMail = textoMail + "</tr>"

    textoMail = textoMail + "</table>"
    textoMail = textoMail + "</br>"
    textoMail = textoMail + "Saludos."

    return textoMail

### Python Operators ###
"""
Función que lee los datos de configuración y los disponibiliza en el XCOM
"""
def configLogADRN(**kwargs):
    datos_Config = import_yaml(ruta_Config)
    for item in datos_Config:
        push_data(kwargs, item, datos_Config[item])

""" 
Función que consulta los datos del ADRN y cuenta la cantidad de parámetros.
Las consultas se hacen por vendor, tecnología y template.
Los datos luego se inyectan a la base Influx 'amovil' para poder representarlos en Grafana
"""
def cuentaParamADRN(**kwargs):

    #Traigo los datos de configuración

    task_id = "datos_config"
    ti = kwargs['ti']
    templateList = ti.xcom_pull(key="templateList", task_ids=[task_id])[0]
    vendor = ti.xcom_pull(key="vendor", task_ids=[task_id])[0]
    tech = ti.xcom_pull(key="tech", task_ids=[task_id])[0]

    timestamp = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    dataInflux = []  
    for vendor_q in vendor:
        print(vendorLongName(vendor_q))
        for tecnologia_q in tech:
            for template_temp in templateList:
                #respuesta = ADRN_param(url_select_data, vendor_q, tecnologia_q, template_temp)
                respuesta = Ingeniera.get_adrn(vendor_q, tecnologia_q, template_temp)
                respuesta_data = respuesta["data"]
                if len(respuesta_data) > 0:
                    print (f"{categoria_q}\t{tecnologia_q}\t{vendor_q}\t{len(respuesta_data)}\t{template_temp}")
                    
                    dataInflux.append({
                            'measurement': medicionInflux,
                            'tags': {
                                    'vendor': vendorLongName(vendor_q),
                                    'tecnologia': tecnologia_q,
                                    'template': template_temp,
                                    },
                            'time': timestamp,
                            'fields':{
                                'cantParam': len(respuesta_data)
                            }
                        })

    insert_influxdb(dataInflux, 'amovil')

""" 
Función que consulta el historial de cambios del ADRN.
Las consultas se hacen por vendor y tecnología.
Luego se genera un reporte específico para las fechas desde / hasta y si hubieron cambios, 
el log se guarda y envía por email a un listado predeterminado de destinatarios.
"""
def historyADRN(**kwargs):

    # Traigo los datos de configuración
    task_id = "datos_config"
    ti = kwargs['ti']
    io_path_log = ti.xcom_pull(key="io_path_log", task_ids=[task_id])[0]
    template_q = ti.xcom_pull(key="template_q", task_ids=[task_id])[0]
    vendor = ti.xcom_pull(key="vendor", task_ids=[task_id])[0]
    tech = ti.xcom_pull(key="tech", task_ids=[task_id])[0]

    # Fecha desde cuándo se hace la consulta (última publicación por ejemplo)
    # Esta fecha actualmente se guarda en /home/coder/project/io/cel_amovil/per/lastLogADRN.txt
    
    fechaDesde = MongoManager('amovil_logADRN').make_query({"TAG": "lastLogADRN"})['fecha']

    fechaDesde_date = datetime.strptime(fechaDesde, "%Y-%m-%d %H:%M:%S")

    # Defino el "hasta" con la correción de GMT -3
    fechaHasta = (datetime.now() + timedelta(hours = correccionHora)).strftime('%Y-%m-%d %H:%M:%S')

    # Levanto de la variable global el listado de templates que voy a analizar
    templateMostrar = template_q
    
    registroCambios = []
    fechaHastaTemp = fechaHasta.replace(":","")
    logName = f"Log ADRN desde {fechaDesde} hasta {fechaHasta}.csv" # Defino el nombre del log
    
    MongoManager('amovil_logADRN').update_doc({"TAG": "lastLogADRN"}, {"fecha": fechaHasta})
    print(f"Busco cambios en ADRN desde {fechaDesde} hasta {fechaHasta}")
    
    log = open(io_path_log + logName, "w", encoding='utf-8') #Agregar encoding='utf-8'?

    registros = 0           # Inicializo el contador de registros cambiados

    for v in vendor:
        for t in tech:
            tableName_q = f"{categoria_q}_M_{v}_{t}_PARAMS"
            
            # Se consulta la API de Change History para c/vendor y tecnología y se extrae la info
            respuesta = ADRN_history(url_history, v, t)
            respuesta_data = respuesta["data"]
            
            # Inicialización de variables
            #registros = 0
            altaParam = 0
            bajaParam = 0
            modParam = 0
            encabezado = False

            # Se cicla dentro de la respuesta buscando los casos que cumplan la condición de fecha        
            for algo in respuesta_data:
                #if fechaDesde < algo["dateTime"]:   #Corregir ésta comparación, no se puede hacer en string!!!
                if fechaDesde_date < datetime.strptime(algo["dateTime"][:algo["dateTime"].find(".")], '%Y-%m-%d %H:%M:%S'): #Corregido
                    if not encabezado:
                        print("\nBase,Fecha,Evento,Elemento,Objeto,Parametro,Parametro Adicional,Template")
                        log.write("\nBase,Fecha,Evento,Elemento,Objeto,Parametro,Parametro Adicional,Template\n")
                        encabezado = True
                    
                    # Preparación de campos para salida
                    evento = algo["eventType"]
                    fecha = algo["dateTime"]
                    
                    # Reemplazo los 'null' que vienen en el campo de info por "NA" para evitar conflictos con dic
                    info = eval(algo["entityInfo"].replace("null,","\"NA\",").replace("null}","\"NA\"}"))
                    
                    elemento = info["element"]
                    objeto = info["object"].replace("\/","/")
                    parametro = info["paramName"]
                    parametroAdicional = info["additionalParam"]
                    if "template" in info:
                        template = info["template"]
                    else:
                        template = "NA"

                    # Verifico template antes de imprimir la línea y registrar el tipo de cambio
                    if template in templateMostrar or "*" in templateMostrar:
                        print(f"{tableName_q},{fecha},{evento},{elemento},{objeto},{parametro},{parametroAdicional},{template}")
                        log.write(f"{tableName_q},{fecha},{evento},{elemento},{objeto},{parametro},{parametroAdicional},{template}\n")                    
                        if "EVENT_ADD" in algo.values():
                            altaParam += 1
                        elif "EVENT_REMOVE" in algo.values():
                            bajaParam += 1
                        elif "EVENT_UPDATE" in algo.values():
                            modParam += 1
        
                        registros += 1
            
            # Armo una lista de diccionarios con el resumen de cambios
            temporal = dict(tabla=tableName_q, altas=altaParam, bajas=bajaParam, modificaciones=modParam)
            registroCambios.append(temporal)
                
    print("\n")
    
    # Imprimo resumen de cambios al final del log

    log.write("\nResumen de cambios del período\n")
    for datos in registroCambios:
        print(datos)
        log.write(f"{str(datos)}\n")

    log.close()

    print(f"#registros: {registros}")

    if registros == 0:          # Si no hubo cambios, borro el log temporal y envío por XCOM ""
        print("Borrar log")
        remove(io_path_log + logName)
        #return ""
        push_data(kwargs, "archivo", "")
    else:                       # Si hubo cambios, envío por XCOM el nombre del log (archivo) creado para que se envíe por correo
        #return logName
        push_data(kwargs, "archivo", logName)
        push_data(kwargs, "resumen", registroCambios)


#def build_email(**context):
def build_email(**kwargs):
    #archivo = pull_data(context,"history_ADRN") 
    task_id = "history_ADRN"
    ti = kwargs['ti']
    archivo = ti.xcom_pull(key="archivo", task_ids=[task_id])[0]
    resumenParaMail = ti.xcom_pull(key="resumen", task_ids=[task_id])[0]
    #print(resumenParaMail)
    #print(type(resumenParaMail))

    task_id = "datos_config"
    ti = kwargs['ti']
    io_path_log = ti.xcom_pull(key="io_path_log", task_ids=[task_id])[0]

    datosForm = TAMBO.GetForms('amovil_logADRN')
    destinatarios = datosForm["email"]
    print(f"Listado de destinatarios en FORM.IO: {destinatarios}")
    
    if archivo == "":
        asunto = f"Log ADRN - Este mail solo se envia a efectos de TESTING"
        print(f"Asunto: {asunto}")
        texto = f"Este correo que se envia a {destinatarios}, se remueve luego de esta etapa de TESTING del DAG"
        emailPara = destinatarios
        sentEmail = False
    else:
        asunto = f"Log ADRN {datetime.now().strftime('%d/%m/%Y')}"
        print(f"Asunto: {asunto}")

        texto = correoHTML(archivo, resumenParaMail)
        ######################
        #texto = f"Se adjunta '{archivo}'<br><br>El resumen de cambios es el siguiente:<br><br>"
        #for datos in resumenParaMail:
        #    texto = texto + str(datos) + "<br>"
        ######################
        print(texto)
        #texto = f"Se adjunta {archivo}"
        adjunto = io_path_log + archivo
        emailPara = destinatarios
        sentEmail = True

    if sentEmail:
        email_op = EmailOperator(
            task_id='send_email',
            to = emailPara,
            subject = asunto,
            html_content = texto,
            files=[adjunto],
        )
        email_op.execute(kwargs)
        print("Se envió email")
    else:
        email_op = EmailOperator(
            task_id='send_email',
            to = ['LCIuale@teco.com.ar'],
            subject = asunto,
            html_content = texto
        )
        email_op.execute(kwargs)
        print("No se envía email")

###################################### 1) TASKS  ######################################

ini = DummyOperator(
        task_id='inicio', 
        retries=1, 
        dag=dag
    )

config = PythonOperator(
    task_id= 'datos_config',
    python_callable = configLogADRN,
    dag=dag
)

load_data_ADRN = PythonOperator(
    task_id ='load_data_ADRN',
    python_callable = cuentaParamADRN,
    dag=dag
)

history_ADRN = PythonOperator(
    task_id ='history_ADRN',
    python_callable = historyADRN,
    dag=dag
)

email_op_python = PythonOperator(
    task_id="python_send_email", 
    python_callable=build_email, 
    provide_context=True, 
    dag=dag
)

fin = DummyOperator(
        task_id='final', 
        retries=1, 
        dag=dag
    )


#################################### 4) WORKFLOW  #####################################

ini >> config >> load_data_ADRN >> fin
ini >> config >> history_ADRN >> email_op_python >> fin