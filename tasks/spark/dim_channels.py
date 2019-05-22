import platform
from dali_dimensions.dim_channels import GenerateDimChannels

if __name__ == '__main__':
    print('Python version:', platform.python_version())
    print('Start the DIM channel generation process')
    GenerateDimChannels().__call__()
    print('Finished the DIM channel generation process')
