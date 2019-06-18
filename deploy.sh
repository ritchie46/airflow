#!/usr/bin/env bash

echo "${SSH_KEY}" > enx-ec2.pem
chmod 400 enx-ec2.pem

echo "Connecting to ubuntu@$IP_ADDRESS..."

rsync -Pav -e "ssh -i enx-ec2.pem -o StrictHostKeyChecking=no" ./ ubuntu@${IP_ADDRESS}:/home/ubuntu/airflow/