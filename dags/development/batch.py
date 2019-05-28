from datetime import datetime
from airflow import DAG

DEFAULT_ARGS = {
    'owner': 'ritchie',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'emr_dag_example',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once'
)

