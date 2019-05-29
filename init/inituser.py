from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
import os
user = PasswordUser(models.User())
user.username = os.environ['AIRFLOW_USERNAME']
user.superuser = True

print('Made user', user.username)

user.email = 'new_user_email@example.com'

try:
    user.password = os.environ['AIRFLOW_PASSWORD']
    session = settings.Session()
    session.add(user)
    session.commit()
    session.close()
except Exception:
    pass
exit()
