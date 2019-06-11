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

## Variables
Within your DAGs you can access (secret) variables such as connection strings and passwords. 
- create locally a JSON file, for instance `variables.json` with the following content:
```
{
    env_variables:{
        "host":"mydatabase.net", 
        "key":"my-secret"
        }
}
```

- Upload this JSON file to the UI of Airflow (tab Admin)

Variables should preferably be acquired at runtime of the dag. This can be done through Jinja templating.

```python
t1 = BashOperator(
    task_id='print_date',
    bash_command='echo "secret value is "',
    dag=dag)
```

If you choose to access the variables at the dag creation time, this will lead to extra database queries and errors
during development. 

Don't do it like this!
```python
from airflow.models import Variable

dag_config = Variable.get("env_variables", deserialize_json=True)
db_host = dag_config['host']
db_key = dag_config['key']


with SparkSteps(DEFAULT_ARGS, dag, instance_count=1) as ss:
    ss.add_spark_job(local_file=local_file, key=local_file, jobargs=[db_host, db_key])

```


## Starting on server
If you have problems and you need to restart `docker-compose` on the airflow server, set the following environment
variables first:

```text
AIRFLOW_USERNAME
AIRFLOW_PASSWORD
FERNET_KEY
POSTGRES_USER
POSTGRES_PASSWORD
```

Once they are set. Docker-compose can be started:

`docker-compose -f prod.yml up -d`


