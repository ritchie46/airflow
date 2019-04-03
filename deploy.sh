#!/usr/bin/env bash

rsync -Pav -e "ssh -i $HOME/.ssh/aws-enx/enx-ec2.pem" ./ ubuntu@$(cat terraform/ip_address.txt):/var/app/docker-airflow/