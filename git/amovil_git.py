"""
Proceso para actualizar manualmente o de manera agendada el pull de los repositorios de las celulas en los entornos
"""
import logging
import os
from datetime import datetime

import airflow
import jinja2
from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DAG_OWNER_NAME = "tambo"
ALERT_EMAIL_ADDRESSES = ['automation@teco.com.ar']
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get(
    "airflow_log_cleanup__max_log_age_in_days", 7
)

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    DAG_ID,
    schedule_interval= None,
    default_args=default_args,
    template_undefined=jinja2.Undefined
)

#tareas

amovil_git_pull = BashOperator(
task_id= "amovil_git_pull",
bash_command="cd /usr/local/tambo/cels/cel_amovil; git pull",
dag=dag
)

amovil_git_pull