# docker-airflow

## requirements
* docker
* docker-compose

## Run locally

The development environment assumes you have a `~/.aws` folder locally with credentials.

Start a local airflow server

`$ docker-compose -f dev.yml up`

If you run it from Windows, remove this line from `dev.yml`: `- ./init:/init`
`


