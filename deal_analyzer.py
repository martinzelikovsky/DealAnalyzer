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
from keepa_client import KeepaAPI


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
        self.keepa_client: KeepaAPI = arg_dict['keepa_client']

        self.RESULT_TAB_REGEX = f'{self.tab_regex}_result'
        self.CHECKPOINT_CSV_REGEX = f'{self.tab_regex}_result'

        self.output_file: str = None
        self.current_tab: str = None
        self.current_asin: str = None
        self.current_csv: str = None
        self.CSV_SCHEMA = {}  # TODO define a datatype schema for reading 

        config_logger(self.output_dir, self.log_name, logger)
        logger.info('Initialized logger for DealAnalyzer.')
    
    def run(self):
        if not self.checkpoint_file:
            self.checkpoint_file = self.input_files[0]
        path_basename, ext = os.path.splitext(self.checkpoint_file)
        self.output_file = f'{path_basename}_result{ext}'
        result_file_exists = os.path.exists(self.output_file)
        if result_file_exists:
            # get current Tab and ASIN 
            self.current_tab = self.get_tab_checkpoint()
            self.current_csv = self.get_current_csv()
            self.current_df = self.get_current_df()
            self.current_asin = self.get_asin_checkpoint()

        # iterate over the current and remaining tabs
        # for each tab sort the asins and continue from the current asin
        input_file_list = self.input_files()[self.input_files.index(self.checkpoint_file):]

        for file in input_file_list:
            excel = pd.ExcelFile(file)
            tab_list = list(filter(lambda x: re.match(self.tab_regex, x), excel.sheet_names))
            if self.current_tab:
                remaining_tabs = tab_list[tab_list.index(self.current_tab):]
            else: 
                self.current_tab = tab_list[0]
                remaining_tabs = tab_list
            for tab in remaining_tabs:
                sheet_df = excel.parse(tab).sort_values('B00 ASIN')  # TODO: specify dtype
                for i, asin in enumerate(sheet_df['B00 ASIN']):
                    keepa_df = self.keepa_client.get_asin_df(asin)  # TODO: continue after implementing keepa client
                    self.current_df = sheet_df.merge(keepa_df, how='left', left_on=['B00 ASIN'], right_on=['asin'])
                    if i % 10 == 0:
                        self.current_df.to_csv(self.current_csv)  # Checkpoint save
                self.current_df.to_csv(self.current_csv)  # Checkpoint save
                # TODO: Save CSV into an Excel book
                writer = pd.ExcelWriter(self.output_file, engine='xlsxwriter')  
                self.current_df.to_excel(writer, sheet_name=self.current_tab, index=False)
    
    def get_current_df(self) -> pd.DataFrame:
        df = None
        if os.path.exists(self.current_csv):
            df = pd.read_csv(self.current_csv)  # TODO: specify dtype map for correct parsing

        return df


    def get_current_csv(self) -> str:
        filename = os.path.join(self.output_dir, f'{self.current_tab}_checkpoint.csv')
        return filename

    def get_result_tab_name(self, tab_num: int) -> str:
        return f'{self.RESULT_TAB_REGEX.replace('\d+', f"{tab_num}")}'

    def get_tab_checkpoint(self) -> str:
        tab_checkpoint = None
        output_file = pd.ExcelFile(self.output_file)
        result_sheets = filter(lambda x: re.match(self.RESULT_TAB_REGEX, x), output_file.sheet_names)
        if result_sheets:
            max_tab = max(result_sheets, key=lambda x: int(re.search('\d+', x).group()))
            tab_checkpoint = self.get_result_tab_name(max_tab + 1)  # TODO: add check to confirm tabs are done

        return tab_checkpoint
    
    def get_asin_checkpoint(self) -> str:
        # read file and find the ASIN at the last line
        asin_checkpoint = None
        if self.current_df is not None:
            asin_checkpoint = self.current_df['asin'][-1]  # TODO: verify
        logger.info(f'ASIN checkpoint {asin_checkpoint=}')

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
    
