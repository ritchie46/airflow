from datetime import datetime
from airflow import DAG
from modules.emr import SparkSteps
from airflow.models import Variable
import os
import pendulum

local_tz = pendulum.timezone("Europe/Amsterdam")

DAG_NAME = 'DALIDQ'

DEFAULT_ARGS = {
    'owner': 'datascienceteam1',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 31, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False
}


if os.environ.get('ENX_PRODUCTION', '1') == '1':
    dag = DAG(DAG_NAME, schedule_interval='0 3 * * *', catchup=False, default_args=DEFAULT_ARGS)

    dag_config = Variable.get("variables_dalidq_2.0", deserialize_json=True)

    gitlab_url = "git+https://{}:{}@{}".\
        format(dag_config['gitlab_username'], dag_config['gitlab_token'], dag_config['gitlab_host'])

    with SparkSteps(DEFAULT_ARGS, dag, instance_count=11,
                    bootstrap_script='tasks/bootstrapping/odbc.sh',
                    subnet_id='subnet-bbc351f3',
                    bootstrap_requirements_yum=['git-core'],
                    bootstrap_requirements_python_with_version={'pandas': '0.24.2'},
                    bootstrap_requirements_python_without_version=[gitlab_url]) as ss:
        ss.add_spark_job(local_file='tasks/spark/dali_preprocess_json2parquet.py',
                         key='dali_preprocess_json2parquet.py',
                         action_on_failure='TERMINATE_CLUSTER')
        ss.add_spark_job(local_file='tasks/spark/dim_box.py', key='dim_box.py',
                         action_on_failure='CANCEL_AND_WAIT',
                         jobargs=[dag_config['S3_URL_DIM_Boxes'], dag_config['S3_URL_pre'],
                                  dag_config['S3_URL_Meta_Boxes'], dag_config['database_SDS'],
                                  dag_config['username_SDS'], dag_config['password_SDS']])
        ss.add_spark_job(local_file='tasks/spark/dim_channels.py', key='dim_channel.py',
                         action_on_failure='TERMINATE_CLUSTER',
                         jobargs=[dag_config['S3_URL_DIM_Channels'], dag_config['database_SDS'],
                                  dag_config['username_SDS'], dag_config['password_SDS']])
        ss.add_spark_job(local_file='tasks/spark/pre_boxchannel.py', key='pre_boxchannel.py',
                         action_on_failure='TERMINATE_CLUSTER',
                         jobargs=[dag_config['S3_URL_DIM_Boxes'], dag_config['S3_URL_DIM_Channels'],
                                  dag_config['S3_URL_BoxChannel']])
        ss.add_spark_job(local_file='tasks/spark/fact_completeness.py', key='fact_completeness.py',
                        action_on_failure='TERMINATE_CLUSTER')
        ss.add_spark_job(local_file='tasks/spark/fact_gaps.py', key='fact_gaps.py',
                         action_on_failure='TERMINATE_CLUSTER')