from datetime import datetime
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
# custom operator plugins
from airflow.operators import EmrStepSensor, UploadFiles, FindSubnet
import hashlib


class SparkSteps:
    def __init__(self, default_args, dag, instance_count=1, master_instance_type='m4.xlarge',
                 slave_instance_type='m4.xlarge', ec2_key_name='enx-ec2',
                 bootstrap_requirements_python_with_version=None,
                 bootstrap_requirements_python_without_version=None,
                 bootstrap_requirements_yum=None, bucket='enx-ds-airflow'):
        """
        Utility class that helps with a synchronous dag for EMR.

        :param default_args: (dict) Default args for the dag.
        :param dag: (DAG)
        :param instance_count: (int) Count of all the instances. Master + Slaves
        :param master_instance_type: (str) Type of ec2-machines. See: https://aws.amazon.com/ec2/instance-types/
        :param slave_instance_type: (str) Type of ec2-machines. See: https://aws.amazon.com/ec2/instance-types/
        :param ec2_key_name: (str) Name of the ec2 key. This allows you to ssh into the EMR cluster.
        :param bootstrap_requirements_python_with_version: (dict) Python libraries needed for the EMR tasks.
                                              { 'numpy': '1.15.4'
                                                'psycopg2' : '0.9' }
        :param bootstrap_requirements_python_without_version: (list) Python libraries needed for the EMR tasks.
                                              ['numpy', 'pandas']
        :param bootstrap_requirements_yum: (list) Containing extra system requirements. Example:
                                                ['unixODBC-devel', 'gcc-c++']
        :param bucket: (str) s3 bucket where the EMR tasks will be copied to. Recommended to leave default.
        """
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
        self.bootstrap_requirements_python_with_version = bootstrap_requirements_python_with_version
        self.bootstrap_requirements_python_without_version = bootstrap_requirements_python_without_version
        self.bootstrap_requirements_yum = bootstrap_requirements_yum
        self.bootstrap_key = None

    def __enter__(self):
        s = "#!/bin/bash\nset -e\nset -x\n"
        if self.bootstrap_requirements_yum is not None:
            s += "\nsudo yum -y install {}\n".format(' '.join(self.bootstrap_requirements_yum))

        s += "sudo pip-3.6 install -U \\\nawscli \\\nboto3 \\"

        if self.bootstrap_requirements_python_with_version is not None:
            s += ('\n' + ' \\ \n'.join("{}=={}".format(key, val) for key, val in
                                       self.bootstrap_requirements_python_with_version.items())) + ' \\'

        if self.bootstrap_requirements_python_without_version is not None:
            for package in self.bootstrap_requirements_python_without_version:
                s += f'\n{package} \\'

        self.bootstrap_key = f"bootstrap_scripts/{hashlib.sha1(bytes(s, 'utf-8')).hexdigest()}.sh"

        self.tasks = [
            UploadFiles(
                task_id='upload-bootstrap-actions',
                dag=self.dag,
                bucket=self.bucket,
                keys=[self.bootstrap_key],
                local_files=[s],
                file_type='str'
            ),

            FindSubnet(
                task_id='find_subnet',
                dag=self.dag
            ),
            EmrCreateJobFlowOperator(
                task_id='create_EMR_cluster',
                # region_name='eu-west-1a',
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/EMR.html#EMR.Client.run_job_flow
                # can set bootstrap actions etc.
                job_flow_overrides={
                    'ReleaseLabel': 'emr-5.23.0',
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
                                'Path': f's3://{self.bucket}/{self.bootstrap_key}',
                                'Args': ['']
                            }
                        },
                    ],
                },
                dag=self.dag
            )]
        return self

    def add_spark_job(self, local_file, key, jobargs=(), action_on_failure='TERMINATE_CLUSTER'):
        """

        :param local_file: (str) path to local python file.
        :param key: (str) Location in S3 cluster.
        :param jobargs: (tpl/ list) containing job arguments as strings
        :param action_on_failure: (str) 'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE'
        """
        EMR_steps = [
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
                task_id=f'add_EMR_steps_{self.job_count}',
                job_flow_id="{{ task_instance.xcom_pull('create_EMR_cluster', key='return_value') }}",
                steps=EMR_steps,
                dag=self.dag
            ),
            EmrStepSensor(
                task_id=f'watch_step_{self.job_count}',
                job_flow_id="{{ task_instance.xcom_pull('create_EMR_cluster', key='return_value') }}",
                step_id_xcom=f'add_EMR_steps_{self.job_count}',
                dag=self.dag
            )
        ]
        self.job_count += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tasks += [
            EmrTerminateJobFlowOperator(
                task_id='remove_cluster',
                job_flow_id="{{ task_instance.xcom_pull('create_EMR_cluster', key='return_value') }}",
                dag=self.dag
            )
        ]
        for i in range(1, len(self.tasks)):
            self.tasks[i - 1].set_downstream(self.tasks[i])


