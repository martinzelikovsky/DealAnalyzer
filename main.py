import argparse
import datetime
import pandas
import re
import keepa
import os
import logging
import sys
import yaml
from pathlib import Path

from utils import config_logger
from keepa_client import KeepaAPI
from deal_analyzer import DealAnalyzer


logger = logging.getLogger(__name__)

def load_config(config_path) -> dict:
    if not Path(config_path).exists():
        return {}
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def parse_args() -> argparse.Namespace:
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
    parser.add_argument('--output_dir', type=str, default=exec_params.get('output_dir', 'results'),
                        help='Base directory for output results')
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

def get_input_files(arg_dict) -> list[Path]:
    # Read and order input excel files by filename
    input_dir = arg_dict.get('input_dir', '.')
    expanded_dir = Path(input_dir).expanduser()
    
    if not expanded_dir.exists():
        raise FileNotFoundError(f'{expanded_dir} does not exist.')
    
    files = []
    for f in expanded_dir.iterdir():
        if f.is_file() and f.suffix.lower() == '.xlsx':
            files.append(f.resolve())
    if not files:
        raise ValueError(f'No input files found. Check {input_dir} for .xlsx files.')
    # Sort files alphabetically by name
    files.sort(key=lambda x: x.name)
    arg_dict['input_file_list'] = [str(f) for f in files]

    return files

def get_output_dir(arg_dict) -> Path:
    # Get or create output directory relative to main.py
    input_files = arg_dict['input_file_list']
    # input_files contains strings, convert to Path to get stem
    base_filenames = [re.sub(r'\s', '_', Path(f).stem) for f in input_files]
    run_name = "_".join(base_filenames)
    
    # Base output path (either absolute or relative to script)
    base_output = Path(arg_dict.get('output_dir', 'results'))
    if not base_output.is_absolute():
        script_dir = Path(__file__).resolve().parent
        base_output = script_dir / base_output
    
    run_output_dir = base_output / run_name

    if not run_output_dir.exists():
        run_output_dir.mkdir(parents=True, exist_ok=True)
    arg_dict['output_dir'] = str(run_output_dir)

    return run_output_dir

def main():
    args = parse_args()
    arg_dict = vars(args)

    # Load full config for extra params
    config = load_config(args.config)
    output_config = config.get('output_config', {})
    enrichment_cols = output_config.get('enrichment_cols', {})
    
    try:
        input_files = get_input_files(arg_dict)
        output_dir = get_output_dir(arg_dict)
        
        config_logger(str(output_dir), arg_dict['log_name'], logger)
        
        logger.info(f"Arguments parsed: {args}")
        logger.info(f"Found {len(input_files)} input files:")
        for f in input_files:
            logger.info(f"  - {f}")
        logger.info(f"Output directory set to: {output_dir}")
        
        keepa_client = KeepaAPI(
            output_dir=str(output_dir),
            log_name=arg_dict['log_name'],
            domain=arg_dict['domain'],
            cache_max_age_days=arg_dict['lookback_days'],
            config_enrichment_cols=enrichment_cols
        )
        
        arg_dict['keepa_client'] = keepa_client
        deal_analyzer = DealAnalyzer(arg_dict)
        deal_analyzer.run()
        
    except Exception as e:
        if logger.handlers:
            logger.exception("Application error:")
        else:
            print(f"Error before logger initialized: {e}")
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
