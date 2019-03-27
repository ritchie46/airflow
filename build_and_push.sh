#!/bin/sh

repository=enx-dataschience-airflow
tag=781327374347.dkr.ecr.eu-west-1.amazonaws.com/${repository}

# authorization token for docker
aws ecr get-login --no-include-email | sh

echo "building $tag"
docker build  -t ${tag} .

echo "pushing to $tag"
docker push ${tag}

if [ $? -gt 0 ]; then  # if exit code last command any other than 0
    aws ecr create-repository --repository-name ${repository}
    docker push ${tag}
fi
