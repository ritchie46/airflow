#!/usr/bin/env bash

: "${POSTGRES_HOST:="postgres"}"
: "${POSTGRES_PORT:="5432"}"
: "${POSTGRES_USER:="airflow"}"
: "${POSTGRES_PASSWORD:="airflow"}"
: "${POSTGRES_DB:="airflow"}"


wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" < /dev/null; do
    j=$((j+1))
    if [ $j -ge 20 ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}


cp /config/airflow.cfg ~/airflow.cfg
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
export AIRFLOW_HOME="/usr/local/airflow"
export PYTHONPATH="/modules:$PYTHONPATH"

wait_for_port "postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"

airflow initdb
python /init/inituser.py
airflow scheduler &
airflow webserver


