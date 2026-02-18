import os
# import pytest


def test_get_output_dir(mocker):
    mocker.patch('os.makedirs')
    from main import get_output_dir

    arg_dict = {'input_file_list': ['/tmp/YYZ1 22DEC25.xlsx'], 'output_dir': 'results'}
    correct_output_dir = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     '..',
                     'results',
                     'YYZ1_22DEC25')
    )

    ret = get_output_dir(arg_dict)
    print(f'{ret=}\n{correct_output_dir}')
    assert ret == correct_output_dir
