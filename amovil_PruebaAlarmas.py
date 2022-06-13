import os
from datetime import datetime, timedelta
from airflow import DAG

from jdbc_big_query_plugin.operators.fmc_operator import FMCDataLakeQuery

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

default_args = {
    'owner': 'amovil',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    #'catchup': False
    'email': ['tambo_core@teco.com.ar'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    #'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}

dag = DAG(
    dag_id = DAG_ID,
    schedule_interval = None,
    catchup = False,
    description='Pruebas de consultas de alarmas FMC en datalake',
    default_args = default_args
)


do_query = FMCDataLakeQuery(
    dest_file="/io/cel_amovil/tmp/Alarmas/test_fmc_query.json",
    date= 20220302,
    columns=['Node', 'AlertKey', 'Summary', 'URL', 'NetworkDesc2', 'StateChange', 'FirstOccurrence', 'LastoOcurrence', 'Class', 'Grade', 'EventId', 'ServerSerial',
             'NetworkDesc4', 'NetworkDesc5', 'NetcoolEventAction'],
    filter_by={'Agent': 'MA_HUAWEI_U2000_NFV'},
    task_id="fmc_datalake_query",
    dag=dag
)

do_query