import argparse
import datetime
import pandas
import re
import keepa
import os
import logging

from utils import config_logger


logger = logging.getLogger(__name__)

class DealAnalyzer:
    
    def __init__(self, arg_dict):
        self.arg_dict = arg_dict
        config_logger(arg_dict, logger)
        logger.info('Initialized logger for DealAnalyzer.')

    def get_checkpoint_asin(self):
        pass

    def get_keepa_client(self):
        pass

    def load_input_file(self):
        pass

    def create_result_file(self):
        pass

    def main(self):
        pass
    
