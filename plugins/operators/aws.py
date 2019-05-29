from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
import boto3
from datetime import datetime


class FindSubnet(BaseOperator):

    @apply_defaults
    def __init__(self, subnet_index=0, pass_subnet=None, *args, **kwargs):
        """
        Retrieve a subnet id in you .aws region.

        :param subnet_index: The index of the subnet you want to return. 0 is the first subnet.
        :param pass_subnet: (str) A subnet Id. Can be used to pass a subnet through if a certain dag structure uses
                                  FindSubnet, but wants to be able to pass a subnet id.
        """
        super().__init__(*args, **kwargs)
        self.subnet_index = subnet_index
        self.pass_subnet = pass_subnet

    def execute(self, context):
        if self.pass_subnet is not None:
            return self.pass_subnet
        client = boto3.client('ec2')
        return client.describe_subnets()['Subnets'][self.subnet_index]['SubnetId']


class UploadFiles(BaseOperator):

    @apply_defaults
    def __init__(self, local_files, bucket, keys, replace=True, file_type='path', *args, **kwargs):
        """

        :param local_files: (list) with paths or file contents
        :param bucket: (str)
        :param keys: (str)
        :param replace: (bool) Replace the file in S3
        :param file_type: (str) 'path'|'str'
        """
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.keys = keys
        self.replace = replace
        self.local_files = local_files
        self.file_type = file_type

    def execute(self, context):
        for key, f in zip(self.keys, self.local_files):
            s3 = S3Hook()
            if self.file_type == 'str':
                s3.load_string(f, key, self.bucket, self.replace)
            else:
                s3.load_file(f, key, self.bucket, self.replace)


class AWSBatchRegisterJobDefinition(BaseOperator):

    @apply_defaults
    def __init__(self, image, command=None, vcpus=1, memory=1024, job_role_arn=None, environment=None, *args, **kwargs):
        """

        :param image: (str) The image used to start a container.
        :param command: (list) The command that is passed to the container.
                               Example: 'echo hello world' -> ["echo", "hello", "world"]
        :param vcpus: (int) The number of vCPUs reserved for the container.
        :param memory: (int) The hard limit (in MiB) of memory to present to the container.
        :param job_role_arn: The Amazon Resource Name (ARN) of the IAM role that the container can assume
                           for AWS permissions.
        :param environment: (list) Containing key value environment variables in dictionary form.
        """
        super().__init__(*args, **kwargs)
        self.job_definition_name = f"airflow-batch-{str(datetime.now().timestamp()).replace('.', '-')}"
        self.image = image
        self.command = command
        self.vcpus = vcpus
        self.memory = memory
        self.job_role_arn = job_role_arn
        self.environment = environment

        self.container_kwargs = dict()
        if job_role_arn is not None:
            self.container_kwargs['jobRoleArn'] = job_role_arn
        if environment is not None:
            self.container_kwargs['environment'] = environment

    def execute(self, context):
        client = boto3.client('batch')
        client.register_job_definition(
            jobDefinitionName=self.job_definition_name,
            type='container',
            containerProperties=dict(
                image=self.image,
                command=self.command,
                vcpus=self.vcpus,
                memory=self.memory,
                **self.container_kwargs
            )
        )
        return self.job_definition_name


class AWSBatchDeregisterJobDefinition(BaseOperator):
    template_fields = ('job_definition_name', )

    def __init__(self, job_definition_name, *args, **kwargs):
        """
        :param job_definition_name: (str)
        """
        super().__init__(*args, **kwargs)
        self.job_definition_name = job_definition_name

    def execute(self, context):
        client = boto3.client('batch')
        client.deregister_job_definition(
            jobDefinition=self.job_definition_name
        )


class AWSBatchOperatorTemplated(AWSBatchOperator):
    template_fields = ('overrides', 'job_name', 'job_definition')

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
