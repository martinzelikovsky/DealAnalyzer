import argparse
import datetime
import pandas as pd
import re
import keepa
import os
import logging

from utils import config_logger


logger = logging.getLogger(__name__)
DOMAIN = 'CA'

class KeepaAPI:
    def __init__(self, output_dir: str, filename: str):
        config_logger(output_dir, filename, logger)
        self.api = self.get_api()
    
    def get_api():
        return keepa.Keepa(os.environ.get('KEEPA_KEY'))

    def query_asin(self, asin: str, **kwargs) -> list:
        ret = None
        try: 
            ret = self.api.query(asin, **kwargs)
        except Exception as e:
            logger.error(f'Failed to query Keepa. {e}')

        return ret
    
    def get_asin_df(self, asin: str, **kwargs) -> pd.DataFrame:
        ret = None

    def check_cache(self, asin) -> str:
        ret = None

    def cache_result(self, result: pd.DataFrame):
        pass

    def get_current_price():
        pass

    def get_epoch_seconds_from_keepa_min(keepa_min):
        return (keepa_min + 21564000) * 60

    def get_date_from_keepa_min(keepa_min):
        return datetime.date.fromtimestamp((keepa_min + 21564000) * 60)

