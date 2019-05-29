import platform
import sys
from dali_facts.fact_completeness import CalculateCompleteness

if __name__ == '__main__':
    print('Python version:', platform.python_version())
    print('Start the calculation of the completeness process')
    CalculateCompleteness().__call__()
    print('Finished the calculation of the completeness process')