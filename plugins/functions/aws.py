from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3


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
        if self.pass_subnet is None:
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
