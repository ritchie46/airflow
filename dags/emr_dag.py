from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

# custom operator plugins
from airflow.operators import EmrStepSensor, UploadFiles, FindSubnet


BUCKET = 'enx-ds-airflow'
SPARK_JOB_KEY = 'my-sparkjob.py'
S3_URI = f's3://{BUCKET}/{SPARK_JOB_KEY}'
CLUSTER_NAME = f"airflow-cluster-{str(datetime.now()).replace(' ','_')}"
LOG_URI = f's3://{BUCKET}/logs/{CLUSTER_NAME}'

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

find_subnet = FindSubnet(
    task_id='find_subnet',
    dag=dag
)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    # region_name='eu-west-1a',
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow
    # can set bootstrap actions etc.
    job_flow_overrides={
        'VisibleToAllUsers': True,  # needed for sensor
        'Name': CLUSTER_NAME,
        'LogUri': LOG_URI,
        'Instances': {
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'InstanceCount': 1,
            'TerminationProtected': False,
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2SubnetId': "{{ task_instance.xcom_pull('find_subnet', key='return_value') }}"
        }
    },
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_emr_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    steps=[
        {
            'Name': 'setup - copy files',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
            }
        },

        {
            'Name': 'Run Spark',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', f'/home/hadoop/{SPARK_JOB_KEY}']
            }
        }

    ],
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_emr_steps', key='return_value')[0] }}",
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    dag=dag
)

upload_script = UploadFiles(
    task_id='upload_script',
    local_files=['tasks/spark/test.py'],
    bucket=BUCKET,
    keys=[SPARK_JOB_KEY],
    replace=True,
    dag=dag
)

find_subnet >> cluster_creator >> step_adder >> step_checker >> cluster_remover
upload_script >> step_adder
