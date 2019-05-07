#!/usr/bin/env bash

# we don't want this to run in gitalab ci
: ${POSTGRES_HOST:="postgres"}
: ${POSTGRES_USER:="airflow"}
: ${POSTGRES_PASSWORD:="airflow"}
: ${POSTGRES_DB:="airflow"}
: ${AIRFLOW_HOME:="/usr/local/airflow"}
export AIRFLOW_HOME

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  local n=20
  while ! nc -z "$host" "$port" < /dev/null; do
    j=$((j+1))
    if [[ ${j} -ge ${n} ]]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$n"
    sleep 5
  done
}

if [[ -z ${RUNNING_CI} ]]; then
    cp /config/airflow.cfg ~/airflow.cfg
    export PYTHONPATH="/modules:$PYTHONPATH"
else
    cp config/airflow.cfg ~/airflow.cfg
    export PYTHONPATH="modules:$PYTHONPATH"
    AIRFLOW_HOME=$(pwd)
    POSTGRES_HOST=${postgres_service}
    echo "$postgres_service"
fi

export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/$POSTGRES_DB"

if [[ -z ${RUNNING_CI} ]]; then
    wait_for_port "postgres" "$POSTGRES_HOST" 5432
fi

airflow initdb
python /init/inituser.py

if [[ -z ${RUNNING_CI} ]]; then
    echo "start scheduler & webserver..."
    airflow scheduler &
    airflow webserver
else
    echo "test dag list..."
    airflow list_dags | grep emr_dag_example > /dev/null
    exit $?
fi

