#!/usr/bin/env bash

export PYTHONPATH="/modules:$PYTHONPATH"
airflow initdb
python /init/inituser.py
airflow scheduler &
airflow webserver


