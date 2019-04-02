from airflow.sensors.base_sensor_operator import BaseSensorOperator, BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
import boto3
from airflow.utils.decorators import apply_defaults


# Standard EmrStepSensor did not seem to work.
class EmrStepSensor(BaseSensorOperator):
    template_fields = ['job_flow_id', 'step_id']
    NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE', 'CANCEL_PENDING']
    FAILED_STATE = ['CANCELLED', 'FAILED', 'INTERRUPTED']

    @apply_defaults
    def __init__(self, job_flow_id, step_id, *args, **kwargs):
        super().__init__(*args, **kwargs, poke_interval=3, soft_fail=False, mode='poke')
        self.emr = boto3.client('emr')
        self.job_flow_id = job_flow_id
        self.step_id = step_id

    def poke(self, context):
        response = self.emr.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = response['Step']['Status']['State']

        if state in self.NON_TERMINAL_STATES:
            return False

        if state in self.FAILED_STATE:
            raise AirflowException('EMR job failed')

        return True


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
        for key, fn in zip(self.keys, self.local_files):
            S3Hook().load_file(fn, key, self.bucket, self.replace)
