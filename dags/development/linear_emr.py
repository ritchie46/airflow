from datetime import datetime
from airflow import DAG
from modules.emr import SparkSteps

DAG_NAME = 'DEV_linear_emr_dag'

DEFAULT_ARGS = {
    'owner': 'ritchie',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(DAG_NAME, schedule_interval='@once', default_args=DEFAULT_ARGS)

with SparkSteps(DEFAULT_ARGS, dag, instance_count=1) as ss:

    ss.add_spark_job(local_file='tasks/development/diagnostics.py', key='diagnostics.py',
                     jobargs=['2019', '4', '8'], action_on_failure='CANCEL_AND_WAIT')
