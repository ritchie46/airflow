#!/usr/bin/env bash

gitlab-runner exec docker \
--env SSH_KEY="$(cat ~/.ssh/aws-enx/enx-ec2.pem)" \
--env AWS_CREDENTIALS="$(cat ~/.aws/credentials)" \
test
