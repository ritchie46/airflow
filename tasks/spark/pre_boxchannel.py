import platform
import sys
from dali_dimensions.pre_boxchannel import CreatePreBoxChannel

if __name__ == '__main__':
    print('Python version:', platform.python_version())
    print('Starting PRE table box-channel generation process')
    input_settings = {
        "URL_source_box": sys.argv[1],
        "URL_source_channel": sys.argv[2],
        "URL_output_file": sys.argv[3]
        }
    pretable = CreatePreBoxChannel(**input_settings)
    sel_box, sel_chan = pretable.read_input()
    sel_box_chan = pretable.create_crossjoin(sel_box, sel_chan)
    pretable.write_output(sel_box_chan)
    print('Finished PRE table box-channel generation process')
