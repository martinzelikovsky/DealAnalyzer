from pathlib import Path
# import pytest


def test_get_output_dir(mocker):
    # Mock mkdir to prevent actual directory creation
    mocker.patch('pathlib.Path.mkdir')
    
    from main import get_output_dir

    # input_file_list expects strings as per main.py implementation
    arg_dict = {'input_file_list': ['/tmp/YYZ1 22DEC25.xlsx'], 'output_dir': 'results'}
    
    # Expected: root/results/YYZ1_22DEC25
    # test_main.py is in tests/, so parent.parent is root
    root_dir = Path(__file__).resolve().parent.parent
    correct_output_dir = root_dir / 'results' / 'YYZ1_22DEC25'

    ret = get_output_dir(arg_dict)
    
    print(f'{ret=}\n{correct_output_dir=}')
    # Compare resolved paths
    assert ret.resolve() == correct_output_dir.resolve()

def test_get_input_files():
    from main import get_input_files

    # Use actual test file
    test_dir = Path(__file__).parent / 'test_input_dir'
    arg_dict = {'input_dir': str(test_dir)}

    ret = get_input_files(arg_dict)
    print(f'{ret=}')
    
    assert len(ret) == 1
    # Check that we found the expected file
    found_file = Path(ret[0])
    assert found_file.name == 'YYZ1 25DEC25.xlsx'
    assert found_file.exists()
