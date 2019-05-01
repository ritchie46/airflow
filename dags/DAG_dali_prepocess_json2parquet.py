from datetime import datetime

from airflow import DAG

from modules.emr import SparkSteps

DAG_NAME = 'DAG_dali_preprocess_json2parquet'

DEFAULT_ARGS = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(DAG_NAME, schedule_interval=None, default_args=DEFAULT_ARGS)

with SparkSteps(DEFAULT_ARGS, dag, instance_count=11) as ss:
    ss.add_spark_job(local_file='tasks/spark/dali_preprocess_json2parquet.py', key='dali_preprocess_json2parquet.py',
                     action_on_failure='CANCEL_AND_WAIT')
