import datetime
import pandas as pd
import keepa
import os
import logging
import json
from pathlib import Path
from utils import config_logger

logger = logging.getLogger(__name__)

DOMAIN_MAP = {
    'US': 1,
    'GB': 2,
    'DE': 3,
    'FR': 4,
    'JP': 5,
    'CA': 6,
    'IT': 8,
    'ES': 9,
    'IN': 10,
    'MX': 11,
    'BR': 12,
    'AU': 13
}

class KeepaAPI:
    def __init__(self, output_dir: str, log_name: str, domain: str = 'CA', cache_max_age_days: int = 7, enable_cache: bool = True, config_enrichment_cols: dict = None):
        config_logger(output_dir, log_name, logger)
        self.api_key = os.environ.get('KEEPA_KEY')
        if not self.api_key:
            logger.error("KEEPA_KEY environment variable not set.")
        
        self.domain_str = domain
        self.domain_id = DOMAIN_MAP.get(domain.upper(), 1)
        self.cache_max_age_days = cache_max_age_days
        self.enable_cache = enable_cache
        self.config_enrichment_cols = config_enrichment_cols or {}
        self.cache_dir = Path.cwd() / 'cache'
        
        if self.enable_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            
        self.api = keepa.Keepa(self.api_key) if self.api_key else None

    def _get_cache_path(self, asin: str) -> Path:
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        return self.cache_dir / f"{asin}_{today}.json"

    def _read_from_cache(self, asin: str) -> list[dict] | None:
        if not self.enable_cache:
            return None
        
        if not self.cache_dir.exists():
            return None
            
        files = [f for f in self.cache_dir.iterdir() if f.is_file() and f.name.startswith(asin) and f.name.endswith('.json')]
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
            with newest_file.open('r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read cache for {asin}: {e}")
            return None

    def _write_to_cache(self, asin: str, data: list[dict]) -> None:
        if not self.enable_cache:
            return
        
        cache_path = self._get_cache_path(asin)
        try:
            with cache_path.open('w') as f:
                json.dump(data, f)
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
                    raw_response = self.api.query(asin, domain=self.domain_id, stats=stats, history=history)
                    data = raw_response
                    self._write_to_cache(asin, data)
                except Exception as e:
                    logger.error(f"Failed to query Keepa for {asin}: {e}")
                    continue
            
            if data:
                results.extend(data)
        
        return results

    def get_results_dataframe(self, product_data: list[dict]) -> pd.DataFrame:
        if not product_data:
            return pd.DataFrame()
            
        processed_data = []
        for product in product_data:
            row = {}
            row['asin'] = product.get('asin')
            
            # Extract fields based on config
            for col, dtype in self.config_enrichment_cols.items():
                val = product.get(col)
                
                # Special handling for some fields
                if col == 'category_tree' and isinstance(product.get('categoryTree'), list):
                    val = " > ".join([c.get('name', '') for c in product.get('categoryTree')])
                elif col == 'sales_rank':
                    val = product.get('salesRank', val)
                elif col == 'min_price' and 'stats' in product:
                    # Index 1 is 'New'
                    min_val = product['stats'].get('min', [None, None])[1]
                    if min_val is not None and min_val > 0:
                        val = min_val / 100.0
                elif col == 'max_price' and 'stats' in product:
                    max_val = product['stats'].get('max', [None, None])[1]
                    if max_val is not None and max_val > 0:
                        val = max_val / 100.0
                elif col == 'avg_price' and 'stats' in product:
                    avg_val = product['stats'].get('avg', [None, None])[1]
                    if avg_val is not None and avg_val > 0:
                        val = avg_val / 100.0
                
                row[col] = val
            
            processed_data.append(row)
            
        df = pd.DataFrame(processed_data)
        
        # Apply types
        for col, dtype in self.config_enrichment_cols.items():
            if col in df.columns:
                try:
                    if dtype == 'int':
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    elif dtype == 'float':
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
                    elif dtype == 'str':
                        df[col] = df[col].astype(str)
                except Exception as e:
                    logger.warning(f"Failed to cast column {col} to {dtype}: {e}")
                    
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
