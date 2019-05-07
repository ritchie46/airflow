# docker-airflow

## requirements
* docker
* docker-compose

## Run locally

The development environment assumes you have a `~/.aws` folder locally with credentials.

Start a local airflow server

`$ docker-compose -f dev.yml up`

If you run it from Windows, remove this line from `dev.yml`: `- ./init:/init`

## Custom Python packages
It is recommended to package python utility code in gitlab. By creating pip installable links with gitlab [deploy
tokens](https://docs.gitlab.com/ee/user/project/deploy_tokens/) we can install them in the bootstrap actions. 
Below is shown an example dag for this purpose. Note that `git` is required for this, so add that to the
`yum` bootstrap steps.

```python
with SparkSteps(DEFAULT_ARGS, dag,
                bootstrap_requirements_yum=['git-core'],
                bootstrap_requirements_python_with_version=['git+https://<username>:<deploy-token>@gitlab.com/repo.git']
                ) as ss:
```

## ODBC driver
A working ODBC driver (needed for `pyodbc`), takes extra bootstrapping. For this purpose the `SparkSteps` context, 
needs to be started with a custom bootstrap script.

```python
with SparkSteps(DEFAULT_ARGS, dag, bootstrap_script='tasks/bootstrapping/odbc.sh') as ss:
```


## Static IP-address (for whitelisting)
If 3rd parties need to specifically whitelist our AWS services, we need to have a static IP adress. I won't bore you
with the technicalities, but this can be achieved by specifying the subnet id when starting a `SparkSteps` context.

Note that this is a private subnet, which means that there is no inbound traffic possible so als no ssh traffic.

```python
with SparkSteps(DEFAULT_ARGS, dag, subnet_id='subnet-bbc351f3') as ss:
```


## Connection secrets
Connections can be added via the UI or programmatically. In DAGs we can access the connection information via
airflow hooks.

```python
from airflow.hooks.base_hook import BaseHook
pw = BaseHook.get_connection('example_connection').password

some_operator(host='secret', password=pw)
```