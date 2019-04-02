#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
sudo pip install -U \
  awscli            \
  boto3

#sudo yum install -y python-psycopg2