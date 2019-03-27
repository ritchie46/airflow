import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
user = PasswordUser(models.User())
user.username = 'enx-ds'
user.email = 'new_user_email@example.com'
user.password = 'KUwTSY9V79#Kxm#d$vbkU'
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()
