import platform
import sys
from dali_dimensions.dim_box import GenerateDimBox

if __name__ == '__main__':
    print('Python version:', platform.python_version())
    print('Start the DIM box generation process')
    input_settings = {
        "S3_URL": sys.argv[1],
        "S3_URL_pre": sys.argv[2],
        "S3_URL_meta": sys.argv[3],
        "database": sys.argv[4],
        "username": sys.argv[5],
        "password": sys.argv[6]
        }
    GenerateDimBox(**input_settings).__call__()
    print('Finished the DIM box generation process')