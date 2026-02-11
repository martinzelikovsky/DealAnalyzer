import argparse
import datetime
import pandas
import re
import keepa
import os
import logging
import sys
import yaml

from utils import config_logger
from deal_analyzer import DealAnalyzer


logger = logging.getLogger(__name__)

def load_config(config_path):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def parse_args():
    # First pass to get the config file path
    temp_parser = argparse.ArgumentParser(add_help=False)
    temp_parser.add_argument('--config', type=str, default='config.yaml')
    temp_args, _ = temp_parser.parse_known_args()
    
    config = load_config(temp_args.config)
    exec_params = config.get('execution_params', {})
    
    parser = argparse.ArgumentParser(description='Deal Analyzer Tool')
    parser.add_argument('--config', type=str, default='config.yaml',
                        help='Path to the configuration file (default: config.yaml)')
    
    # Use config values as defaults
    parser.add_argument('--platform', type=str, default=exec_params.get('platform', 'unix'),
                        help='Platform: windows or unix')
    parser.add_argument('--input_dir', type=str, default=exec_params.get('input_dir', '~/deal_analyzer_input'),
                        help='Directory for input files')
    parser.add_argument('--output_dir', type=str, default=exec_params.get('output_dir', './results/$date_result'),
                        help='Directory for output files')
    parser.add_argument('--lookback_days', type=int, default=exec_params.get('lookback_days', 30),
                        help='Number of days to look back for historical data')
    parser.add_argument('--domain', type=str, default=exec_params.get('domain', 'CA'),
                        help='Marketplace domain (e.g., CA, US)')
    parser.add_argument('--log_name', type=str, default=exec_params.get('log_name', 'deal_analyzer.log'),
                        help='Filename of generated log.')
    
    # Input config overrides
    input_config = config.get('input_config', {})
    parser.add_argument('--tab_regex', type=str, default=input_config.get('tab_regex', '^Detail_\\d+'),
                        help='Regex for excel tabs to process')

    return parser.parse_args()

def initialize_run(arg_dict):
    pass

def get_input_files(arg_dict):
    # Read and order input excel files by filename
    input_dir = arg_dict.get('input_dir', '.')
    expanded_dir = os.path.expanduser(input_dir)
    
    if not os.path.exists(expanded_dir):
        raise FileNotFoundError(f'{expanded_dir} does not exist.')
    
    files = []
    for f in os.listdir(expanded_dir):
        if f.lower().endswith(('.xlsx', '.xls')):
            files.append(os.path.abspath(os.path.join(expanded_dir, f)))
    
    # Sort files alphabetically by name
    files.sort()
    arg_dict['input_file_list'] = files

    return files

def get_output_dir(arg_dict):
    # Get or create output directory
    input_files = arg_dict['input_file_list']
    base_filenames = [os.path.splitext(os.path.basename(f))[0] for f in input_files]
    output_name = "_".join(base_filenames)
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), output_name)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    arg_dict['output_dir'] = output_dir

    return output_dir

def get_checkpoint_file(arg_dict):
    result_files = list(filter(lambda x: 'result' in x, os.listdir(arg_dict['output_dir'])))
    if not result_files:
        arg_dict['checkpoint_file'] = None
        logger.info(f'Fresh run - no checkpoint found.')
        return

    checkpoint_file = os.path.abspath(max(result_files))
    arg_dict['checkpoint_file'] = checkpoint_file
    logger.info(f'Continuing an older run - result file {checkpoint_file} found.')

    return checkpoint_file


def main():
    args = parse_args()
    arg_dict = vars(args)
    input_files = get_input_files(arg_dict)
    output_dir = get_output_dir(arg_dict)
    config_logger(output_dir)
    logger.info(f"Arguments parsed: {args}")
    logger.info(f"Found {len(input_files)} input files:")
    for f in input_files:
        logger.info(f"  - {f}")
    logger.info(f"Output directory set to: {output_dir}")
    get_checkpoint_file(arg_dict)
    deal_analyzer = DealAnalyzer(arg_dict)


if __name__ == '__main__':
    main()