from airflow.plugins_manager import AirflowPlugin
from .sensors import aws as aws_sensors
from .operators import aws as aws_operators


class AWSPlugin(AirflowPlugin):
    name = 'aws_plugin'
    sensors = [
        aws_sensors.EmrStepSensor
    ]
    operators = [
        aws_operators.FindSubnet,
        aws_operators.UploadFiles,
        aws_operators.AWSBatchRegisterJobDefinition,
        aws_operators.AWSBatchDeregisterJobDefinition,
        aws_operators.AWSBatchOperatorTemplated
    ]

