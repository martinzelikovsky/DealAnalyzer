import datetime
import pandas as pd
import keepa
from keepa import Domain
import os
import logging
from pathlib import Path
import pickle

from .utils import config_logger

logger = logging.getLogger(__name__)


class KeepaAPI:
    def __init__(self, output_dir: str, log_name: str, domain: str = 'CA', cache_max_age_days: int = 7, enable_cache: bool = True, config_enrichment_cols: dict = None, enrichment_col_prefix: str = 'keepa_'):
        config_logger(output_dir, log_name, logger)
        self.api_key = os.environ.get('KEEPA_KEY')
        if not self.api_key:
            logger.error("KEEPA_KEY environment variable not set.")
        
        self.domain = Domain[domain.upper()]
        self.cache_max_age_days = cache_max_age_days
        self.enable_cache = enable_cache
        self.config_enrichment_cols = config_enrichment_cols or {}
        self.enrichment_col_prefix = enrichment_col_prefix
        self.cache_dir = Path.cwd() / 'cache'
        
        if self.enable_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            
        self.api = keepa.Keepa(self.api_key) if self.api_key else None

    def _get_cache_path(self, asin: str) -> Path:
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        return self.cache_dir / f"{asin}_{today}.pickle"

    def _read_from_cache(self, asin: str) -> dict | None:
        if not self.enable_cache:
            return None
        
        if not self.cache_dir.exists():
            return None
            
        files: list[Path] = [f for f in self.cache_dir.iterdir() if f.is_file() and f.name.startswith(asin) and f.name.endswith('.pickle')]
        if not files:
            return None
            
        # Get the newest one based on modification time or name
        # Assuming filenames contain dates, sorting by name is safest for "latest logical file"
        # but sorting by mtime is safer for "latest modified file".
        # Let's stick to name sort since the date is in the filename YYYY-MM-DD
        files.sort(key=lambda x: x.name)
        newest_file = files[-1]
        
        file_time = datetime.datetime.fromtimestamp(newest_file.stat().st_mtime)
        if (datetime.datetime.now() - file_time).days > self.cache_max_age_days:
            return None
            
        try:
            with newest_file.open('rb') as f:
                data = pickle.load(f)
                return data
        except Exception as e:
            logger.error(f"Failed to read cache for {asin}: {e}")
            return None

    def _write_to_cache(self, asin: str, data: dict) -> None:
        if not self.enable_cache:
            return
        
        cache_path = self._get_cache_path(asin)
        try:
            with cache_path.open('wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to write cache for {asin}: {e}")

    def get_product_data(self, asins: list[str], stats: int = 30, history: bool = True) -> list[dict]:
        results = []
        for asin in asins:
            data = self._read_from_cache(asin)
            if data is None:
                if not self.api:
                    logger.error(f"Keepa API not initialized, cannot fetch {asin}")
                    continue
                try:
                    logger.info(f"Fetching {asin} from Keepa API...")
                    # The keepa SDK query returns a list of products
                    raw_response = self.api.query(asin, domain=self.domain, stats=stats, history=history, progress_bar=False)
                    data = raw_response[0] if isinstance(raw_response, list) and raw_response else None
                    if data:
                        self._write_to_cache(asin, data)
                except Exception as e:
                    logger.error(f"Failed to query Keepa for {asin}: {e}")
                    continue
            
            if data:
                results.append(data)
        
        return results

    def get_results_dataframe(self, product_data: list[dict]) -> pd.DataFrame:
        if not product_data:
            return pd.DataFrame()
            
        processed_data = []
        prefix = self.enrichment_col_prefix

        for product in product_data:
            row = {}
            row['asin'] = product.get('asin')
            
            # Extract fields based on config
            # config_enrichment_cols is a dict of {col_name: type}
            # We iterate over the requested columns
            for col, dtype in self.config_enrichment_cols.items():
                add_date: bool = False
                price_date: str = ''
                
                # Default fetch
                val = product.get(col)
                
                # Special handling for some fields
                if col == 'categoryTree' and isinstance(product.get('categoryTree'), list):
                    val = " > ".join([c.get('name', '') for c in product.get('categoryTree')])
                elif col == 'salesRank':
                     # salesRank is a dict in the raw response, usually we want the current rank for the main category
                     # But sometimes it's an int if already processed. 
                     # The raw Keepa 'salesRanks' is a dict {categoryId: rank, ...}
                     # The SDK might expose 'salesRank' as the rank in the main category?
                     # Let's trust the key exists or fallback to the dict 'salesRanks'
                     val = product.get('salesRank') or product.get('salesRanks')
                elif col == 'minPrice' and 'stats' in product:
                    # Index 0 is 'Amazon', Index 1 is 'New'
                    min_val = product['stats'].get('min', [None, None])[0]
                    if min_val is not None and min_val[1] > 0:
                        price_date = self.get_date_from_keepa_min(min_val[0])
                        val = min_val[1] / 100.0
                elif col == 'maxPrice' and 'stats' in product:
                    max_val = product['stats'].get('max', [None, None])[0]
                    if max_val is not None and max_val[1] > 0:
                        price_date = self.get_date_from_keepa_min(min_val[0])
                        val = max_val[1] / 100.0
                elif col == 'avgPrice' and 'stats' in product:
                    avg_val = product['stats'].get('avg', [None, None])[0]
                    if avg_val is not None and avg_val > 0:
                        val = avg_val / 100.0
                elif col == 'minIntervalPrice' and 'stats' in product:
                    min_val = product['stats'].get('minInInterval', [None, None])[0]
                    if min_val is not None and min_val[1] > 0:
                        price_date = self.get_date_from_keepa_min(min_val[0])
                        val = min_val[1] / 100.0
                elif col == 'maxIntervalPrice' and 'stats' in product:
                    max_val = product['stats'].get('maxInInterval', [None, None])[0]
                    if max_val is not None and max_val[1] > 0:
                        price_date = self.get_date_from_keepa_min(max_val[0])
                        val = max_val[1] / 100.0

                # Apply prefix to the output column name
                row[f"{prefix}{col}"] = val
                if price_date:
                    row[f"{prefix}{col}Date"] = price_date

            processed_data.append(row)
            
        df = pd.DataFrame(processed_data)
        
        # Apply types
        for col, dtype in self.config_enrichment_cols.items():
            prefixed_col = f"{prefix}{col}"
            if prefixed_col in df.columns:
                try:
                    if dtype == 'int':
                        df[prefixed_col] = pd.to_numeric(df[prefixed_col], errors='coerce').fillna(0).astype(int)
                    elif dtype == 'float':
                        df[prefixed_col] = pd.to_numeric(df[prefixed_col], errors='coerce').astype(float)
                    elif dtype == 'str':
                        df[prefixed_col] = df[prefixed_col].astype(str)
                except Exception as e:
                    logger.warning(f"Failed to cast column {prefixed_col} to {dtype}: {e}")
        # All other columns (dates for now)
        for col in [x for x in df.columns if x not in [f'{prefix}{y}' for y in self.config_enrichment_cols.keys()]]:
            df[col] = df[col].astype(str)

        return df

    def get_asin_df(self, asin: str) -> pd.DataFrame:
        data = self.get_product_data([asin])
        return self.get_results_dataframe(data)

    @staticmethod
    def get_epoch_seconds_from_keepa_min(keepa_min):
        return (keepa_min + 21564000) * 60

    @staticmethod
    def get_date_from_keepa_min(keepa_min):
        return datetime.date.fromtimestamp((keepa_min + 21564000) * 60)
