"""


"""
import os
import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from lib.teco_data_management import *
from datetime import datetime, timedelta
#from lib.L_teco_db import insert_influxdb
#from lib.tambo import *
#import requests
#from airflow.operators.email_operator import EmailOperator
#from os import remove
#from lib.teco_data_management import push_data
from lib.teco_mongodb import MongoManager
import yaml

from lisy_plugin.operators.lisy_operator import *
from teco_db.operators.tecoMongoDbOperator import TecoReadMongo
from lib.teco_mongodb import MongoManager
from csv import reader

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
    schedule_interval = '0 4 * * *',
    catchup = False,
    description = "Actualización diaria inventario móvil",
    tags = ['Inventario Móvil'],
    default_args=default_args
)


#################################### 2) VARIABLES  ####################################

#dumpLisy = "/io/cel_amovil/tmp/Inventario/LisyMaestroCeldas_CRVD.txt"
dumpLisy = "/io/cel_amovil/tmp/Inventario/LisyMaestroCeldas.txt"
#dumpLisy = "/io/cel_amovil/tmp/Inventario/LisyMaestroCeldas_algunosSitios.txt"
estadoOk = ["Active", "PGS", "In Construction", "Blocking", "KPI measurement", "Discarded"]
#estadoOk = ["Active"]

#################################### 3) FUNCTIONS  ####################################
### FUNCIONES INDEPENDIENTES ###
# Función para leer el yaml de configuración
def import_yaml(archivo):
	with open(archivo, 'r') as file:
		Config = yaml.safe_load(file)

	print(f"Datos de configuración: {Config}")

	return Config

def consultaInventario(sitio=None):
    
    if sitio == None:
        consulta = TecoReadMongo(
            task_id='read',
            table='hostname',
            #query = {"ShelfNetworkRole" : "MOBILE ACCESS", 'RackName': sitio},
            query = {"ShelfNetworkRole" : "MOBILE ACCESS"},
            provide_context=True,
            dag=dag
        )
    else:
        sitio_filtro = {'$regex': f'^{sitio}'}
    
        consulta = TecoReadMongo(
            task_id='read',
            table='hostname',
            #query = {"ShelfNetworkRole" : "MOBILE ACCESS", 'RackName': sitio},
            #query = {"ShelfNetworkRole" : "MOBILE ACCESS", 'ShelfName': sitio_filtro},
            query = {"ShelfNetworkRole" : "MOBILE ACCESS", 'NetworkElement': sitio_filtro},
            provide_context=True,
            dag=dag
        )
   
    return consulta.execute()

def get_fields(fila, separador = "|"):

    inicio = 0
    listaCampos = []
    fila = fila.replace("ï»¿Name", "Name")
    fila = fila.replace("\ufeffName", "Name")
    while fila.find(separador, inicio) != -1:
        #test[0:test.find("|")]
        listaCampos.append(fila[inicio: fila.find(separador, inicio)])
        inicio = fila.find(separador, inicio) + 1
    
    return listaCampos

def actualizarInventario(**kwargs):
    
    if os.path.exists(dumpLisy):
      
        ti_c = os.path.getctime(dumpLisy) 
        #ti_m = os.path.getmtime(dumpLisy) 
        c_ti = time.ctime(ti_c) 
        #m_ti = time.ctime(ti_m)

        print(f"Comenzando a procesar el inventario del {c_ti}")

        fila = 1
        # Leo el dump de Lisy (Maestro de Celdas) y lo cargo en la variable 'registro'
        with open(dumpLisy) as file:
            csv_reader = reader(file)
            registro = []
            for row in csv_reader:
                if fila == 1:
                    encabezado = get_fields(row[0])
                    fila += 1
                    #registro.append(encabezado)
                else:
                    filaTemp = get_fields(row[0])
                    filaTemp_dict = {}
                    for x in range(len(encabezado)):
                        filaTemp_dict.update({encabezado[x]: filaTemp[x]})
                    #for algo in filaTemp:
                    #    filaTemp_dict.update({encabezado[filaTemp.index(algo)]: algo})
                    registro.append(filaTemp_dict)
                    fila += 1
                #print(row) 

        file.closed # True

        #print(f"Registro: {registro}")
        print(f"Filas (celdas) en Lisy: {fila}")
        #print(type(registro))
        fila = 1
            
        celdas = []
        diccCeldas ={}
        #regTemp = {}
        fallas = 0
        cont = 0
        breakOk = False

        for algo in registro:
            #print(f"{algo} - {item[algo]}")
            if algo["Administrative Status"] in estadoOk:       # Verifico que sean estados "válidos" de Lisy definidos en la lista 'estadoOk'
                if len(algo["Access Radio"]) > 0:                      # Verifico que el registro contenga datos claves
                    #sitioTemp = algo.pop("Access Radio")
                    #celdaTemp = algo.pop("Name")
                    sitioTemp = algo["Access Radio"]
                    celdaTemp = algo["Name"]
                    # Agregar sitioCode = "Site Code"
                    for celda in celdas: 
                        breakOk = False
                        if sitioTemp in celda.keys():
                            #print(registro[sitioTemp])
                            #regTemp = registro#[sitioTemp]
                            #registro[celdaTemp] = algo
                            celda[sitioTemp][celdaTemp] = algo
                            #celdas.append({sitioTemp: {celdaTemp: algo}})
                            breakOk = True
                            break
                        
                    if not breakOk:    
                        celdas.append({sitioTemp: {celdaTemp: algo}})
                        cont += 1
                    
                    if len(celdas) == 0:
                        celdas.append({sitioTemp: {celdaTemp: algo}})
                        cont += 1
                    #diccTemp = {}.fromkeys([celdaTemp], algo)
                    #celdas.append({celdaTemp: [algo]})
                    #celdas.append({sitioTemp: {celdaTemp: algo}})
                else:
                    nombre = algo['Name']
                    #print(f"Falla en: {algo['Name']} - {algo['Administrative Status']}")
                    fallas += 1
            
        #print(f"Ultimos temporales: {sitioTemp} - {celdaTemp}")
        print(f"Cantidad de celdas sin sitio asociado ('Access Radio' @Lisy): {fallas}")
        print(f"Cantidad de registros: {len(celdas)}")
        print(f"Conteo: {cont}")
        #print(f"Celdas: {celdas}")

        inventarioActual = consultaInventario()
        print(f"Tipo de inventario actual: {type(inventarioActual)}")
        #sitiosInventario = []
        #for reg in inventarioActual:
        #    sitiosInventario.append(reg[NetworkElement])

        #vuelta = 0
        for algunSitio in inventarioActual:
            #if algunSitio["NetworkElement"] == "CRVDTP_1":
            #    print("Eureka!!!!!!!!!!!!!!!!!")
                
            for algunasCeldas in celdas:
            #    if vuelta < 2:
            #        print(algunasCeldas.keys())
            #        vuelta += 1
                if algunSitio["NetworkElement"] in algunasCeldas.keys():
                    #print(algunasCeldas[algunSitio["NetworkElement"]])
                    #print(algunSitio["NetworkElement"])
                    #for key, value in algunSitio.items():
                    #    print(f"{key}: {value}")
                    #MongoManager('hostname').update_doc({"ShelfNetworkRole" : "MOBILE ACCESS", 'NetworkElement': sitio_filtro},{"VARIABLES_CUSTOM": {'amovil': {'var4': '4'}}})
                    algunSitio["VARIABLES_CUSTOM"]['amovil'] = {'LisyMaestroCeldas': algunasCeldas[algunSitio["NetworkElement"]]}
                    #print(algunSitio["VARIABLES_CUSTOM"])
                    #MongoManager('hostname').update_doc({"ShelfNetworkRole" : "MOBILE ACCESS", 'NetworkElement': algunSitio["NetworkElement"]},{"VARIABLES_CUSTOM": {'amovil': algunasCeldas[algunSitio["NetworkElement"]]}})
                    MongoManager('hostname').update_doc({"ShelfNetworkRole" : "MOBILE ACCESS", 'NetworkElement': algunSitio["NetworkElement"]},{"VARIABLES_CUSTOM": algunSitio["VARIABLES_CUSTOM"]})
    else:
        print("No se encontró el archivo")

###################################### 4) TASKS  ######################################

actualizar_inventario = PythonOperator(
    task_id= 'actualizar_inventario_amovil',
    python_callable = actualizarInventario,
    dag=dag
)

#################################### 5) WORKFLOW  #####################################

actualizar_inventario