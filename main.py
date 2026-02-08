import argparse
import datetime
import pandas
import re
import keepa
import os
import logging
import yaml
from deal_analyzer import DealAnalyzer

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
        print(f"Warning: Input directory '{expanded_dir}' does not exist.")
        return []
    
    files = []
    for f in os.listdir(expanded_dir):
        if f.lower().endswith(('.xlsx', '.xls')):
            files.append(os.path.abspath(os.path.join(expanded_dir, f)))
    
    # Sort files alphabetically by name
    files.sort()
    return files

def get_output_dir(arg_dict):
    # Get or create output directory
    input_files = get_input_files(arg_dict)
    output_name = f'{"_".join([x.split("/")[-1].split(".")[0] for x in input_files])}'
    output_dir = os.path.join(os.path.abspath(__file__).replace(__file__, ''),
                              arg_dict['output_dir'], output_name)
    if not os.path.exists(output_dir):
    # This run has not been triggered before
        os.makedirs(output_dir)
    
    return output_dir


def is_new_result_dir_needed():
    return True

def main():
    args = parse_args()
    arg_dict = vars(args)
    print(f"Arguments parsed: {args}")
    
    input_files = get_input_files(arg_dict)
    print(f"Found {len(input_files)} input files:")
    for f in input_files:
        print(f"  - {f}")

if __name__ == '__main__':
    main()