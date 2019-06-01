import platform
import sys
from dali_facts.fact_gaps import CalculateGaps

if __name__ == '__main__':
    print('Python version:', platform.python_version())
    print('Start the calculation of the gaps process')
    CalculateGaps().__call__()
    print('Finished the calculation of the gaps process')