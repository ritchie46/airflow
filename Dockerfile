FROM python:3.7-slim

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
ENV AIRFLOW_GPL_UNIDECODE yes

# Airflow
ARG AIRFLOW_VERSION=1.10.3
ARG AIRFLOW_HOME=/usr/local/airflow
ARG PYTHON_DEPS=""

COPY ./requirements.txt /requirements.txt

# libssl-dev & libffi-dev:  airflow[cryptograph]
# libkrb5-dev:  airflow[kerbero]
# libsasl2-dev: airflow[hive]
# libpq-dev: airlfow[postgres]

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        build-essential \
        curl \
        rsync \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install apache-airflow[crypto,postgres,hive,jdbc]==${AIRFLOW_VERSION} \
    && pip install -r /requirements.txt \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY init /init
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/init/entrypoint.sh"]
# set default arg for entrypoint
CMD ["webserver"]
