from datetime import datetime
from airflow import DAG
from airflow.operators import AWSBatchRegisterJobDefinition, AWSBatchOperatorTemplated, AWSBatchDeregisterJobDefinition

DEFAULT_ARGS = {
    'owner': 'ritchie',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'batch_dev',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once'
)

register_job = AWSBatchRegisterJobDefinition(
    dag=dag,
    task_id='register_job',
    image='busybox',
    command=['echo', 'hello', 'airflow']
)

submit_job = AWSBatchOperatorTemplated(
    dag=dag,
    task_id='submit_job',
    job_name=f"airflow-job",
    job_definition="{{ task_instance.xcom_pull(task_ids='register_job', key='return_value') }}",
    job_queue='airflow-standard-queue',
    overrides={}
)

deregister_job = AWSBatchDeregisterJobDefinition(
    dag=dag,
    task_id='deregister_job',
    trigger_rule='all_done',
    job_definition_name="{{ task_instance.xcom_pull(task_ids='register_job', key='return_value') }}:1"
)

register_job >> submit_job >> deregister_job
