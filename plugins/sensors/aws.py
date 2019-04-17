from airflow.sensors.base_sensor_operator import BaseSensorOperator

from airflow.exceptions import AirflowException
import boto3
from airflow.utils.decorators import apply_defaults


# Standard EmrStepSensor did not seem to work.
class EmrStepSensor(BaseSensorOperator):
    template_fields = ['job_flow_id', 'step_id']
    NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE', 'CANCEL_PENDING']
    FAILED_STATE = ['CANCELLED', 'FAILED', 'INTERRUPTED']

    @apply_defaults
    def __init__(self, job_flow_id, step_id=None, step_id_xcom=None, *args, **kwargs):
        """

        :param job_flow_id: (str) Job flow id in AWS, (get from XCom)
        :param step_id: (str) Step id in AWS, (get from XCom)
        :param step_id_xcom: (str) Name of 'EmrAddStepsOperator' to pull XCom response from. Note:
                            step_id will be overwritten by this.
        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs, poke_interval=3, soft_fail=False, mode='poke')
        self.emr = boto3.client('emr')
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.step_id_xcom = step_id_xcom

    def poke(self, context):
        if self.step_id_xcom is not None:
            task_instance = context['task_instance']
            self.step_id = task_instance.xcom_pull(self.step_id_xcom, key='return_value')[-1]

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



