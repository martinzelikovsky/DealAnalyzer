import json
import os
import datetime
import logging
import pandas as pd
import re
from typing import List, Dict, Any, Optional
from pathlib import Path

from .keepa_client import KeepaAPI
from .utils import config_logger

logger = logging.getLogger(__name__)

class Manifest:
    def __init__(self, output_dir: str):
        self.path = Path(output_dir) / 'state.json'
        self.data = {
            "creation_time": str(datetime.datetime.now()),
            "input_files": [],
            "output_files": [],
            "completed_tabs": {}, # file_path -> [tab_names]
            "current_input_file": None,
            "current_tab": None,
            "current_asin": None,
            "status": "initialized"
        }

    def load(self) -> bool:
        if self.path.exists():
            try:
                with self.path.open('r') as f:
                    self.data = json.load(f)
                return True
            except Exception as e:
                logger.error(f"Failed to load manifest: {e}")
        return False

    def save(self):
        # Use a temporary file in the same directory to ensure atomic move
        temp_path = self.path.with_suffix('.json.tmp')
        try:
            with temp_path.open('w') as f:
                json.dump(self.data, f, indent=4)
            temp_path.replace(self.path)
        except Exception as e:
            logger.error(f"Failed to save manifest: {e}")

    def update_progress(self, input_file: str, tab: str, asin: str):
        self.data["current_input_file"] = str(input_file)
        self.data["current_tab"] = tab
        self.data["current_asin"] = asin
        self.data["status"] = "in_progress"
        self.save()

    def mark_tab_complete(self, input_file: str, tab: str):
        input_file_str = str(input_file)
        if input_file_str not in self.data["completed_tabs"]:
            self.data["completed_tabs"][input_file_str] = []
        if tab not in self.data["completed_tabs"][input_file_str]:
            self.data["completed_tabs"][input_file_str].append(tab)
        self.data["current_asin"] = None
        self.save()

class DealAnalyzer:
    def __init__(self, arg_dict: dict):
        self.arg_dict: dict = arg_dict
        self.output_dir: Path = Path(arg_dict['output_dir'])
        self.staging_dir: str = self.output_dir / 'staging'
        self.tab_regex: str = arg_dict['tab_regex']
        self.input_files: list[Path] = [Path(p) for p in arg_dict['input_file_list']]
        self.keepa_client: KeepaAPI = arg_dict['keepa_client']
        
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        config_logger(arg_dict['output_dir'], arg_dict['log_name'], logger)
            
        self.manifest: Manifest = Manifest(str(self.output_dir))
        if self.manifest.load():
            logger.info("Resuming from existing manifest.")
        else:
            self.manifest.data["input_files"] = [str(p) for p in self.input_files]
            self.manifest.save()

    def run(self):
        for file_path in self.input_files:
            # Skip completed files if all tabs are done
            
            excel = pd.ExcelFile(file_path)
            all_tabs = [t for t in excel.sheet_names if re.match(self.tab_regex, t)]
            
            completed_in_file = self.manifest.data["completed_tabs"].get(str(file_path), [])
            
            for tab in all_tabs:
                if tab in completed_in_file:
                    logger.info(f"Tab {tab} already completed for {file_path}. Skipping.")
                    continue
                
                self.process_tab(file_path, tab, excel)
                self.manifest.mark_tab_complete(str(file_path), tab)

        self.finalize()

    def process_tab(self, file_path: Path, tab: str, excel_obj: pd.ExcelFile):
        logger.info(f"Processing Tab: {tab} in {file_path.name}")
        
        # Set initial context in manifest
        self.manifest.data["current_input_file"] = str(file_path)
        self.manifest.data["current_tab"] = tab
        self.manifest.save()

        sheet_df = excel_obj.parse(tab).sort_values('B00 ASIN')
        staging_csv = self.staging_dir / f"{file_path.name}_{tab}.csv"
        
        results = []
        # Resume within tab
        if self.manifest.data["current_tab"] == tab and self.manifest.data["current_input_file"] == str(file_path):
            if staging_csv.exists():
                existing_df = pd.read_csv(staging_csv)
                results = existing_df.to_dict('records')
                last_asin = self.manifest.data.get("current_asin")
                if last_asin:
                    logger.info(f"Resuming {tab} from ASIN: {last_asin}")
                    sheet_df = sheet_df[sheet_df['B00 ASIN'] >= last_asin]
                    results = list(filter(lambda x: x['B00 ASIN'] < last_asin, results))
                else:
                    logger.info(f"Resuming {tab} from start (no ASIN in manifest)")
        
        if sheet_df.empty and results:
            logger.info(f"Tab {tab} already finished processing all ASINs.")
        else:
            for i, (idx, row) in enumerate(sheet_df.iterrows()):
                asin = row['B00 ASIN']
                logger.info(f"Fetching Keepa data for {asin}")
                
                keepa_df = self.keepa_client.get_asin_df(asin)
                
                # Convert row to dict for merging
                row_dict = row.to_dict()
                if not keepa_df.empty:
                    keepa_data = keepa_df.iloc[0].to_dict()
                    row_dict.update(keepa_data)
                
                results.append(row_dict)
                
                # Periodic checkpoint
                if len(results) % 10 == 0:
                    pd.DataFrame(results).to_csv(staging_csv, index=False)
                    self.manifest.update_progress(str(file_path), tab, asin)

        # Final save for tab
        if results:
            final_df = pd.DataFrame(results)
            final_df.to_csv(staging_csv, index=False)
            # Update manifest with last ASIN in result set
            last_asin = results[-1].get('B00 ASIN')
            self.manifest.update_progress(str(file_path), tab, last_asin)

    def finalize(self):
        logger.info("Finalizing: Stitching staged files into Excel report.")
        
        if not self.input_files:
            return

        # Determine the name for the final report based on the first input file
        report_name = self.input_files[0].name.replace('.xlsx', '_result.xlsx')
        report_path = self.output_dir / report_name
        
        with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
            for file_path in self.input_files:
                completed_tabs = self.manifest.data["completed_tabs"].get(str(file_path), [])
                for tab in completed_tabs:
                    staging_csv = self.staging_dir / f"{file_path.name}_{tab}.csv"
                    if staging_csv.exists():
                        df = pd.read_csv(staging_csv)
                        df.to_excel(writer, sheet_name=f"{tab}_result", index=False)
        
        self.manifest.data["output_files"] = [str(report_path)]
        self.manifest.data["status"] = "completed"
        self.manifest.save()
        logger.info(f"Report generated: {report_path}")
