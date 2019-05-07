from airflow import settings
from airflow.models import Connection

conn = Connection(
    conn_id='example_connection',
    conn_type='ssh',
    host='honeypot',
    login='user',
    password='password',
    port=8888
)

session = settings.Session()
session.add(conn)
session.commit()