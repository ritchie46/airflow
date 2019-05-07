import platform
import sys
import time
import botocore.vendored.requests as requests

print('Python version:', platform.python_version())
print('Python script called with the following sysargs:', sys.argv)

response = requests.get('http://ifconfig.me')
print('Public ip:', response.text)


while True:
    time.sleep(60)
    pass