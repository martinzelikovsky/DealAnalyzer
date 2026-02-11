import argparse
import csv
import datetime
import pandas as pd
import re
import keepa
import os
import logging
from typing import Tuple

from utils import config_logger


logger = logging.getLogger(__name__)

class DealAnalyzer:
    
    def __init__(self, arg_dict: dict):
        self.arg_dict: dict = arg_dict
        self.input_dir: str = arg_dict['input_dir']
        self.output_dir: str = arg_dict['output_dir']
        self.lookback_days: int = arg_dict['lookback_days']
        self.domain: str = arg_dict['domain']
        self.tab_regex: str = arg_dict['tab_regex']
        self.input_files: list[str] = arg_dict['input_file_list']
        self.checkpoint_file: str = arg_dict['checkpoint_file']
        self.log_name: str = arg_dict['log_name']

        self.RESULT_TAB_REGEX = f'{self.tab_regex}_result'

        self.output_file: str = None
        self.current_tab: str = None
        self.current_asin: str = None

        config_logger(self.output_dir, self.log_name, logger)
        logger.info('Initialized logger for DealAnalyzer.')
    
    def run(self):
        current_tab = None
        current_asin = None
        if not self.checkpoint_file:
            self.checkpoint_file = self.input_files[0]
        path_basename, ext = os.path.splitext(self.checkpoint_file)
        self.output_file = f'{path_basename}_result{ext}'
        result_file_exists = os.path.exists(self.output_file)
        if result_file_exists:
            # get current Tab and ASIN 
            current_tab, current_asin = self.get_checkpoint()
        else:
            # create result file
            self.create_result_file()
        
        
        # iterate over the current and remaining tabs
        # for each tab sort the asins and continue from the current asin

    def get_result_tab_name(self, tab_num: int) -> str:
        return f'{self.RESULT_TAB_REGEX.replace('\d+', f"{tab_num}")}'

    def continue_run(self):
        pass

    def start_run(self):
        pass

    def get_checkpoint(self) -> Tuple[str, str]:
        # open and read file
        max_tab = None
        max_asin = None
        output_file = pd.ExcelFile(self.output_file)
        result_sheets = filter(lambda x: re.match(self.RESULT_TAB_REGEX, x), output_file.sheet_names)

        if result_sheets:
            max_tab = self.get_result_tab_name(max(result_sheets, key=lambda x: int(re.search('\d+').group())))
        if max_tab is not None:
            max_asin = self.get_max_asin_from_tab()

        return max_tab, max_asin

    def get_max_asin_from_tab(self, tab_name: str) -> str:
        max_asin = None
        try:
            output_file = pd.ExcelFile(self.output_file)
            sheet = output_file.parse(tab_name)
            max_asin = sheet['asin'].max()
        except Exception as e:
            logger.error(f'Failed to get max ASIN. {e}')

        return max_asin


    def get_keepa_client(self):
        pass

    def load_input_file(self):
        pass

    def create_result_file(self):
        pass

    def main(self):
        pass
    
