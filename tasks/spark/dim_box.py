import platform
from dali_dimensions.dim_box import GenerateDimBox

if __name__ == '__main__':
    print('Python version:', platform.python_version())
    print('Start the DIM box generation process')
    GenerateDimBox().__call__()
    print('Finished the DIM box generation process')