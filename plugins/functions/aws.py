from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3


class FindSubnet(BaseOperator):

    @apply_defaults
    def __init__(self, subnet_index=0, *args, **kwargs):
        """
        Retrieve a subnet id in you .aws region.

        :param subnet_index: The index of the subnet you want to return. 0 is the first subnet.
        """
        super().__init__(*args, **kwargs)
        self.subnet_index = subnet_index

    def execute(self, context):
        client = boto3.client('ec2')
        return client.describe_subnets()['Subnets'][self.subnet_index]['SubnetId']


class UploadFiles(BaseOperator):

    @apply_defaults
    def __init__(self, local_files, bucket, keys, replace=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.keys = keys
        self.replace = replace
        self.local_files = local_files

    def execute(self, context):
        for key, f in zip(self.keys, self.local_files):
            s3 = S3Hook()
            if isinstance(f, str):
                s3.load_string(f, key, self.bucket, self.replace)
            else:
                s3.load_file(f, key, self.bucket, self.replace)
