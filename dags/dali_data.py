from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

# custom operator plugins
from airflow.operators import EmrStepSensor, UploadFiles, FindSubnet

DAG_NAME = 'dali-data-ETL'
BUCKET = 'enx-ds-airflow'
SPARK_JOB_KEY = 'my-sparkjob.py'
S3_URI = f's3://{BUCKET}/{SPARK_JOB_KEY}'
CLUSTER_NAME = f"airflow-cluster-{str(datetime.now()).replace(' ', '_')}"
LOG_URI = f's3://{BUCKET}/logs/{CLUSTER_NAME}'

spark_steps = [{
    'name': 'process-data',
    'spark-job-key': f'{DAG_NAME}_process_data.py',
    'local-file': 'tasks/spark/dali-process-data.py'
}, {
    'name': 'process-data',
    'spark-job-key': f'{DAG_NAME}_check_data_completeness.py',
    'local-file': 'tasks/spark/dali_check_data_completeness.py'
}]

aws_emr_steps = []
local_files = []
for step in spark_steps:
    s3_uri = f"s3://{BUCKET}/{step['spark-job-key']}"
    local_files.append(step['local-file'])

    aws_emr_steps.append(
        {
            'Name': 'setup - copy files',
            'ActionOnFailure': 'TERMINATE_JOB_FLOW',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['aws', 's3', 'cp', s3_uri, '/home/hadoop/']
            }
        })
    aws_emr_steps.append(
        {
            'Name': 'Run Spark',
            'ActionOnFailure': 'TERMINATE_JOB_FLOW',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', f"/home/hadoop/{step['spark-job-key']}"]
            }
        }
    )

DEFAULT_ARGS = {
    'owner': 'ritchie',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(DAG_NAME, schedule_interval='@once', default_args=DEFAULT_ARGS)

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
        },
        'Configurations': [
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        'BootstrapActions': [
            {
                'Name': 'python-dependencies',
                'ScriptBootstrapAction': {
                    'Path': 's3://enx-ds-airflow/bootstrap_emr.sh',
                    'Args': ['']
                }
            },
        ],

    },
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_emr_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    steps=aws_emr_steps,
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

upload_scripts = UploadFiles(
    task_id='upload_scripts',
    local_files=local_files,
    bucket=BUCKET,
    keys=[step['spark-job-key'] for step in spark_steps],
    replace=True,
    dag=dag
)

find_subnet >> cluster_creator >> step_adder >> step_checker >> cluster_remover
upload_scripts >> step_adder
