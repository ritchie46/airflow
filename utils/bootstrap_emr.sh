#!/bin/bash

set -e
set -x

# Non-standard and non-Amazon Machine Image Python modules:
sudo pip-3.4 install -U \
  awscli \
  boto3
#sudo yum install -y python-psycopg2