from airflow.plugins_manager import AirflowPlugin
from .sensors import aws


class AWSPlugin(AirflowPlugin):
    name = 'aws_plugin'
    sensors = [
        aws.EmrStepSensor
    ]
    operators = [
        aws.FindSubnet,
        aws.UploadFile
    ]

