#!/usr/bin/env bash

rsync -Pav -e "ssh -i $HOME/.ssh/aws-enx/enx-ec2.pem" ./ ubuntu@${IP_ADRESS}:/var/app/airflow/