#!/usr/bin/env bash

(return 0 2>/dev/null) && sourced=1 || sourced=0

if [[ sourced -eq 0 ]]; then
    echo "Script should be sourced"
    exit 1
fi

export AIRFLOW_USERNAME=dev
export AIRFLOW_PASSWORD=dev

aws s3 cp utils/bootstrap_emr.sh s3://enx-ds-airflow/bootstrap_emr.sh