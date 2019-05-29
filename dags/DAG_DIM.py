from datetime import datetime
from airflow import DAG
from modules.emr import SparkSteps
from airflow.models import Variable
import os


DAG_NAME = 'DAG_DIM'

DEFAULT_ARGS = {
    'owner': 'datascienceteam1',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

if os.environ.get('PRODUCTION', 1) == 1:
    dag = DAG(DAG_NAME, schedule_interval=None, default_args=DEFAULT_ARGS)

    dag_config = Variable.get("variables_dalidq_2.0", deserialize_json=True)

    gitlab_url = "git+https://{}:{}@{}".\
        format(dag_config['gitlab_username'], dag_config['gitlab_token'], dag_config['gitlab_host'])

    with SparkSteps(DEFAULT_ARGS, dag, instance_count=1,
                    bootstrap_script='tasks/bootstrapping/odbc.sh',
                    subnet_id='subnet-bbc351f3',
                    bootstrap_requirements_yum=['git-core'],
                    bootstrap_requirements_python_with_version={'pandas': '0.24.2'},
                    bootstrap_requirements_python_without_version=[gitlab_url]) as ss:
        ss.add_spark_job(local_file='tasks/spark/dim_box.py', key='dim_box.py',
                         action_on_failure='CANCEL_AND_WAIT',
                         jobargs=[dag_config['S3_URL_DIM_Boxes'], dag_config['database_SDS'],
                                  dag_config['username_SDS'], dag_config['password_SDS']])
        ss.add_spark_job(local_file='tasks/spark/dim_channels.py', key='dim_channel.py',
                         action_on_failure='CANCEL_AND_WAIT',
                         jobargs=[dag_config['S3_URL_DIM_Channels'], dag_config['database_SDS'],
                                  dag_config['username_SDS'], dag_config['password_SDS']])
