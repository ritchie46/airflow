from datetime import datetime
from airflow import DAG
from modules.emr import SparkSteps

DAG_NAME = 'DEV_odbc'

DEFAULT_ARGS = {
    'owner': 'ritchie',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(DAG_NAME, schedule_interval='@once', default_args=DEFAULT_ARGS)

with SparkSteps(DEFAULT_ARGS, dag, instance_count=1, subnet_id='subnet-bbc351f3',
                bootstrap_script='tasks/bootstrapping/odbc.sh') as ss:

    ss.add_spark_job(local_file='tasks/development/db/odbc.py', key='development/db/odbc.py',
                     action_on_failure='CANCEL_AND_WAIT')
