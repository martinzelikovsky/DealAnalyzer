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

        self.RESULT_TAB_SUFFIX = '_result'
        
        self.output_file: str = None
        self.current_tab: str = None
        self.current_asin: str = None
        self.current_csv: str = None
        self.current_df: pd.DataFrame = None

        config_logger(self.output_dir, self.log_name, logger)
        logger.info('Initialized DealAnalyzer.')
    
    def run(self):
        if not self.input_files:
            logger.error("No input files to process.")
            return

        if self.checkpoint_file:
            self.output_file = self.checkpoint_file
            self.current_tab = self.get_tab_checkpoint()
            self.current_asin = self.get_asin_checkpoint()
            logger.info(f"Resuming from {self.current_tab}, ASIN: {self.current_asin}")
        else:
            first_input = self.input_files[0]
            path_basename, ext = os.path.splitext(os.path.basename(first_input))
            self.output_file = os.path.join(self.output_dir, f'{path_basename}_result{ext}')
            logger.info(f"Starting fresh run. Output: {self.output_file}")

        for file in self.input_files:
            # If we have a checkpoint and haven't reached the current tab's file yet, skip
            # (This is simplified, assuming all tabs in one file or multiple files are processed in order)
            excel = pd.ExcelFile(file)
            tab_list = [t for t in excel.sheet_names if re.match(self.tab_regex, t)]
            
            if self.current_tab and self.current_tab.replace(self.RESULT_TAB_SUFFIX, '') not in tab_list:
                # If current_tab is set but not in this file, we might have already processed this file
                # Need better logic if multiple files are used.
                pass

            for tab in tab_list:
                result_tab_name = f"{tab}{self.RESULT_TAB_SUFFIX}"
                
                # If we are resuming and this tab is already done (exists in output and not current), skip
                if self.checkpoint_file and os.path.exists(self.output_file):
                    with pd.ExcelFile(self.output_file) as reader:
                        if result_tab_name in reader.sheet_names and result_tab_name != self.current_tab:
                            logger.info(f"Tab {result_tab_name} already completed, skipping.")
                            continue

                logger.info(f"Processing tab: {tab}")
                self.current_tab = result_tab_name
                self.current_csv = self.get_current_csv_path(tab)
                
                sheet_df = excel.parse(tab).sort_values('B00 ASIN')
                
                # Load existing progress for this tab if any
                if os.path.exists(self.current_csv):
                    self.current_df = pd.read_csv(self.current_csv)
                else:
                    self.current_df = pd.DataFrame()

                for i, asin in enumerate(sheet_df['B00 ASIN']):
                    if self.current_asin and asin <= self.current_asin:
                        continue
                    
                    logger.info(f"Querying Keepa for ASIN: {asin}")
                    keepa_df = self.keepa_client.get_asin_df(asin)
                    
                    # Merge with original row data
                    row_df = sheet_df[sheet_df['B00 ASIN'] == asin]
                    merged_row = row_df.merge(keepa_df, how='left', left_on='B00 ASIN', right_on='asin')
                    
                    self.current_df = pd.concat([self.current_df, merged_row], ignore_index=True)
                    self.current_asin = asin

                    if i % 10 == 0:
                        self.current_df.to_csv(self.current_csv, index=False)
                        logger.info(f"Checkpoint saved to {self.current_csv}")

                self.current_df.to_csv(self.current_csv, index=False)
                
                # Save to Excel
                mode = 'a' if os.path.exists(self.output_file) else 'w'
                if mode == 'a':
                    with pd.ExcelWriter(self.output_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                        self.current_df.to_excel(writer, sheet_name=self.current_tab, index=False)
                else:
                    with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
                        self.current_df.to_excel(writer, sheet_name=self.current_tab, index=False)
                
                logger.info(f"Tab {self.current_tab} saved to {self.output_file}")
                # Reset for next tab
                self.current_asin = None
                if os.path.exists(self.current_csv):
                    os.remove(self.current_csv)

    def get_current_csv_path(self, tab_name: str) -> str:
        return os.path.join(self.output_dir, f'{tab_name}_checkpoint.csv')

    def get_tab_checkpoint(self) -> str:
        if not self.output_file or not os.path.exists(self.output_file):
            return None
        with pd.ExcelFile(self.output_file) as reader:
            result_sheets = [s for s in reader.sheet_names if s.endswith(self.RESULT_TAB_SUFFIX)]
            if not result_sheets:
                return None
            # Return the last one being worked on (assuming alphabetical/sequential order)
            return sorted(result_sheets)[-1]
    
    def get_asin_checkpoint(self) -> str:
        if self.current_tab:
            csv_path = self.get_current_csv_path(self.current_tab.replace(self.RESULT_TAB_SUFFIX, ''))
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                if not df.empty and 'B00 ASIN' in df.columns:
                    return df['B00 ASIN'].iloc[-1]
        return None
