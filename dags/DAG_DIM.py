from datetime import datetime
from airflow import DAG
from modules.emr import SparkSteps

DAG_NAME = 'DAG_DIM'

DEFAULT_ARGS = {
    'owner': 'datascienceteam1',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(DAG_NAME, schedule_interval=None, default_args=DEFAULT_ARGS)
gitlab = 'git+https://gitlab+deploy-token-12:LEHCB6ZnFgJ-xZLXhZrh@enx.gitlab.schubergphilis.com/' \
         'data-science/10_dali-data-quality-2.0.git@packaging-project-dali-dq'

with SparkSteps(DEFAULT_ARGS, dag, instance_count=1,
                bootstrap_script='tasks/bootstrapping/odbc.sh',
                subnet_id='subnet-bbc351f3',
                bootstrap_requirements_yum=['git-core'],
                bootstrap_requirements_python_with_version={'pandas': '0.24.2'},
                bootstrap_requirements_python_without_version=[gitlab]) as ss:
    ss.add_spark_job(local_file='tasks/spark/dim_box.py', key='dim_box.py',
                     action_on_failure='CANCEL_AND_WAIT')
    ss.add_spark_job(local_file='tasks/spark/dim_channels.py', key='dim_channels.py',
                     action_on_failure='CANCEL_AND_WAIT')
