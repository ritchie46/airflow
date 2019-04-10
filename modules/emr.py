from datetime import datetime
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
# custom operator plugins
from airflow.operators import EmrStepSensor, UploadFiles, FindSubnet


class SparkSteps:
    def __init__(self, default_args, dag, instance_count=1, bucket='enx-ds-airflow', master_instance_type='m4.xlarge',
                 slave_instance_type='m4.xlarge', ec2_key_name='enx-ec2'):
        self.default_args = default_args
        self.cluster_name = f"airflow-cluster-{str(datetime.now()).replace(' ', '_')}"
        self.bucket = bucket
        self.instance_count = instance_count
        self.master_instance_type = master_instance_type
        self.slave_instance_type = slave_instance_type
        self.ec2_key_name = ec2_key_name
        self.dag = dag
        self.tasks = []
        self.find_subnet = None
        self.cluster_creator = None
        self.job_count = 0

    def __enter__(self):
        self.tasks = [FindSubnet(
            task_id='find_subnet',
            dag=self.dag
        ),
            EmrCreateJobFlowOperator(
                task_id='create_emr_cluster',
                # region_name='eu-west-1a',
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow
                # can set bootstrap actions etc.
                job_flow_overrides={
                    'VisibleToAllUsers': True,  # needed for sensor
                    'Name': self.cluster_name,
                    'LogUri': f's3://{self.bucket}/logs/{self.cluster_name}',
                    'Instances': {
                        'MasterInstanceType': self.master_instance_type,
                        'SlaveInstanceType': self.slave_instance_type,
                        'InstanceCount': self.instance_count,
                        'TerminationProtected': False,
                        'KeepJobFlowAliveWhenNoSteps': True,
                        'Ec2KeyName': self.ec2_key_name,
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
                dag=self.dag
            )]
        return self

    def add_spark_job(self, local_file, key, jobargs=(), action_on_failure='TERMINATE_CLUSTER'):

        emr_steps = [
            {
                'Name': f'setup - copy files_{local_file}',
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', f"s3://{self.bucket}/{key}", '/home/hadoop/']
                }
            },
            {
                'Name': f'Run_Spark_{key}',
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', f"/home/hadoop/{key}"] + list(jobargs)
                }
            }

        ]

        self.tasks += [
            UploadFiles(
                task_id=f'upload_scripts_{self.job_count}',
                local_files=[local_file],
                bucket=self.bucket,
                keys=[key],
                replace=True,
                dag=self.dag
            ),
            EmrAddStepsOperator(
                task_id=f'add_emr_steps_{self.job_count}',
                job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
                steps=emr_steps,
                dag=self.dag
            ),
            EmrStepSensor(
                task_id=f'watch_step_{self.job_count}',
                job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
                step_id_xcom=f'add_emr_steps_{self.job_count}',
                dag=self.dag
            )
        ]
        self.job_count += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tasks += [
            EmrTerminateJobFlowOperator(
                task_id='remove_cluster',
                job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
                dag=self.dag
            )
        ]
        for i in range(1, len(self.tasks)):
            self.tasks[i - 1].set_downstream(self.tasks[i])



