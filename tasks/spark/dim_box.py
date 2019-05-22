import platform
import sys
from dali_dimensions.dim_box import GenerateDimBox

if __name__ == '__main__':
    print('Python version:', platform.python_version())
    print('Start the DIM box generation process')
    input_settings = {
        "S3_URL": sys.argv[1],
        "database": sys.argv[2],
        "username": sys.argv[3],
        "password": sys.argv[4]
        }
    GenerateDimBox(**input_settings).__call__()
    print('Finished the DIM box generation process')