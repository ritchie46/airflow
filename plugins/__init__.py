from airflow.plugins_manager import AirflowPlugin
from .sensors import aws as aws_sensors
from .functions import aws as aws_functions


class AWSPlugin(AirflowPlugin):
    name = 'aws_plugin'
    sensors = [
        aws_sensors.EmrStepSensor
    ]
    operators = [
        aws_functions.FindSubnet,
        aws_functions.UploadFiles
    ]

