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
        self.batch_size: int = arg_dict.get('batch_size', 20)
        self.input_files: list[Path] = [Path(p) for p in arg_dict['input_file_list']]
        self.keepa_client: KeepaAPI = arg_dict['keepa_client']
        self.summary_sheet_name: str = arg_dict.get('summary_sheet_name', 'Summary')
        self.evaluation_config: dict = arg_dict.get('evaluation_config', {})
        self.distribution_config: dict = arg_dict.get('distribution_config', {})
        self.tab_metadata: dict = {}
        
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
            
            self._parse_summary(file_path, excel)
            
            completed_in_file = self.manifest.data["completed_tabs"].get(str(file_path), [])
            
            for tab in all_tabs:
                if tab in completed_in_file:
                    logger.info(f"Tab {tab} already completed for {file_path}. Skipping.")
                    continue
                
                self.process_tab(file_path, tab, excel)
                self.manifest.mark_tab_complete(str(file_path), tab)

        self.finalize()

    def _parse_summary(self, file_path: Path, excel_obj: pd.ExcelFile):
        if self.summary_sheet_name not in excel_obj.sheet_names:
            logger.warning(f"Summary sheet '{self.summary_sheet_name}' not found in {file_path.name}")
            return
            
        df = excel_obj.parse(self.summary_sheet_name)
        
        header_idx = None
        for idx, row in df.iterrows():
            row_values = [str(v).lower() for v in row.values]
            if any("msrp %" in v for v in row_values) and any("fob cost" in v for v in row_values):
                header_idx = idx
                break
                
        if header_idx is None:
            logger.warning(f"Could not locate header row in {self.summary_sheet_name} for {file_path.name}")
            return
            
        df.columns = df.iloc[header_idx]
        df = df.iloc[header_idx + 1:].reset_index(drop=True)
        df = df.dropna(subset=['Detail'])
        df = df[~df['Detail'].astype(str).str.contains('total', case=False, na=False)]
        
        for _, row in df.iterrows():
            detail_val = str(row.get('Detail', '')).strip()
            if not detail_val:
                continue
            
            # Format tab name: e.g. "1" -> "Detail_1", "1.0" -> "Detail_1"
            if detail_val.endswith('.0'):
                detail_val = detail_val[:-2]
            tab_name = f"Detail_{detail_val}"
            
            try: offer_rate = float(row.get('MSRP %', 0))
            except: offer_rate = 0.0
                
            try: fob_cost = float(row.get('FOB Cost', 0))
            except: fob_cost = 0.0
                
            try: expected_qty = float(row.get('Sum of Quantity', 0))
            except: expected_qty = 0.0
                
            try: expected_msrp = float(row.get('Sum of EXT MSRP', 0))
            except: expected_msrp = 0.0
                
            self.tab_metadata[f"{file_path.name}_{tab_name}"] = {
                'offer_rate': offer_rate,
                'fob_cost': fob_cost,
                'expected_quantity': expected_qty,
                'expected_total_msrp': expected_msrp
            }
        logger.info(f"Parsed {len(self.tab_metadata)} tabs from summary sheet in {file_path.name}")

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
                if results:
                    drop_count = min(self.batch_size, len(results))
                    resume_asin = results[-drop_count]['B00 ASIN']
                    logger.info(f"Resuming {tab} from ASIN: {resume_asin} (rewinding {drop_count} items to repeat batch)")
                    sheet_df = sheet_df[sheet_df['B00 ASIN'] >= resume_asin]
                    results = results[:-drop_count]
                else:
                    logger.info(f"Resuming {tab} from start (no ASINs in staging)")
        
        if sheet_df.empty and results:
            logger.info(f"Tab {tab} already finished processing all ASINs.")
        else:
            for i in range(0, len(sheet_df), self.batch_size):
                batch_df = sheet_df.iloc[i:i+self.batch_size]
                asins = batch_df['B00 ASIN'].tolist()
                
                logger.info(f"Fetching Keepa data for batch of {len(asins)} ASINs")
                keepa_df = self.keepa_client.get_asins_df(asins)
                
                for _, row in batch_df.iterrows():
                    asin = row['B00 ASIN']
                    row_dict = row.to_dict()
                    
                    if not keepa_df.empty and 'asin' in keepa_df.columns:
                        keepa_match = keepa_df[keepa_df['asin'] == asin]
                        if not keepa_match.empty:
                            keepa_data = keepa_match.iloc[0].to_dict()
                            row_dict.update(keepa_data)
                            
                    results.append(row_dict)
                
                # Checkpoint after each batch
                pd.DataFrame(results).to_csv(staging_csv, index=False)
                last_asin = asins[-1]
                self.manifest.update_progress(str(file_path), tab, last_asin)

        # Final save for tab
        if results:
            final_df = pd.DataFrame(results)
            final_df.to_csv(staging_csv, index=False)
            # Update manifest with last ASIN in result set
            last_asin = results[-1].get('B00 ASIN')
            self.manifest.update_progress(str(file_path), tab, last_asin)

    def finalize(self):
        logger.info("Finalizing: Stitching staged files into Excel report and performing evaluation.")
        
        if not self.input_files:
            return

        report_name = self.input_files[0].name.replace('.xlsx', '_result.xlsx')
        report_path = self.output_dir / report_name
        
        margin_rate = self.evaluation_config.get('margin_rate', 0.15)
        value_proxy = self.evaluation_config.get('value_proxy', 'keepa_minInInterval')
        unenriched_sale_rate = self.evaluation_config.get('unenriched_sale_rate', 0.0)
        
        rankings_data = []
        distribution_data = []
        summary_results = []
        
        with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
            for file_path in self.input_files:
                completed_tabs = self.manifest.data["completed_tabs"].get(str(file_path), [])
                
                for tab in completed_tabs:
                    staging_csv = self.staging_dir / f"{file_path.name}_{tab}.csv"
                    if not staging_csv.exists():
                        continue
                        
                    df = pd.read_csv(staging_csv)
                    if df.empty:
                        continue
                        
                    # Metadata for tab
                    metadata_key = f"{file_path.name}_{tab}"
                    metadata = self.tab_metadata.get(metadata_key, {})
                    offer_rate = metadata.get('offer_rate', 0.0)
                    fob_cost = metadata.get('fob_cost', 0.0)
                    expected_qty = metadata.get('expected_quantity', 0.0)
                    
                    # 1. Product-level Grouping and Aggregation
                    # Identify the correct Quantity and MSRP columns
                    qty_col = next((col for col in df.columns if 'Quantity' in col or 'Qty' in col), None)
                    msrp_col = next((col for col in df.columns if 'MSRP' in col and 'EXT' not in col), None)
                    
                    agg_dict = {col: 'first' for col in df.columns if col != 'B00 ASIN'}
                    if qty_col:
                        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
                        agg_dict[qty_col] = 'sum'
                        
                    df_grouped = df.groupby('B00 ASIN', as_index=False).agg(agg_dict)
                    
                    # 2. Product-level Calculations
                    if value_proxy in df_grouped.columns:
                        df_grouped[value_proxy] = pd.to_numeric(df_grouped[value_proxy], errors='coerce').fillna(0)
                        df_grouped['Enriched'] = df_grouped[value_proxy] > 0
                        df_grouped['Estimated_Value'] = df_grouped[value_proxy]
                    else:
                        df_grouped['Enriched'] = False
                        df_grouped['Estimated_Value'] = 0.0
                        
                    df_grouped.loc[~df_grouped['Enriched'], 'Estimated_Value'] = 0.0
                    
                    if msrp_col:
                        df_grouped['Original_MSRP'] = pd.to_numeric(df_grouped[msrp_col], errors='coerce').fillna(0)
                        df_grouped['Unit_Cost'] = df_grouped['Original_MSRP'] * offer_rate
                    else:
                        df_grouped['Original_MSRP'] = 0.0
                        df_grouped['Unit_Cost'] = 0.0
                        
                    df_grouped['Net_Value'] = df_grouped['Estimated_Value'] * (1 - margin_rate)
                    df_grouped['Unit_Profit'] = df_grouped['Net_Value'] - df_grouped['Unit_Cost']
                    
                    # 3. Tab-level Aggregations
                    realized_qty = df_grouped[qty_col].sum() if qty_col else 0
                    unenriched_rate = (~df_grouped['Enriched']).mean() if not df_grouped.empty else 0.0
                    
                    calc_qty = df_grouped[qty_col] if qty_col else 1
                    est_pallet_value = (df_grouped['Net_Value'] * calc_qty).sum()
                    est_pallet_profit = est_pallet_value - fob_cost
                    pallet_roi = (est_pallet_profit / fob_cost * 100) if fob_cost > 0 else 0.0
                    
                    # Append to Rankings
                    rankings_data.append({
                        'File': file_path.name,
                        'Tab': tab,
                        'Estimated Pallet Profit': est_pallet_profit,
                        'Pallet ROI (%)': pallet_roi,
                        'Total Pallet Cost': fob_cost,
                        'Estimated Pallet Value': est_pallet_value,
                        'Realized Quantity': realized_qty,
                        'Expected Quantity': expected_qty,
                        'Unenriched Rate': unenriched_rate
                    })
                    
                    # Append to Summary Results
                    summary_results.append({
                        'File': file_path.name,
                        'Tab': tab,
                        'FOB Cost': fob_cost,
                        'Offer Rate': offer_rate,
                        'Expected Qty': expected_qty,
                        'Realized Qty': realized_qty,
                        'Unenriched Rate': unenriched_rate,
                        'Estimated Pallet Profit': est_pallet_profit,
                        'Pallet ROI (%)': pallet_roi
                    })
                    
                    # Append to Distribution Analysis
                    cat_col = next((c for c in df_grouped.columns if 'categoryTree' in c or 'Category' in c), None)
                    for _, row in df_grouped.iterrows():
                        qty = row[qty_col] if qty_col else 1
                        cat_tree = str(row[cat_col]) if cat_col and pd.notna(row[cat_col]) else 'Unknown'
                        cats = [c.strip() for c in cat_tree.split('>')]
                        main_category = cats[0] if cats and cats[0] else 'Unknown'
                        sub_category = cats[1] if len(cats) > 1 and cats[1] else 'Unknown'
                        
                        distribution_data.append({
                            'File': file_path.name,
                            'Tab': tab,
                            'ASIN': row['B00 ASIN'],
                            'CategoryTree': cat_tree,
                            'Category': main_category,
                            'Subcategory': sub_category,
                            'Original_MSRP': row.get('Original_MSRP', 0.0),
                            'Cost': row['Unit_Cost'] * qty,
                            'Estimated_Profit': row['Unit_Profit'] * qty,
                            'Quantity': qty
                        })
                    
                    # Write individual detail result sheet
                    cols = df_grouped.columns.tolist()
                    calc_cols = ['Enriched', 'Unit_Cost', 'Estimated_Value', 'Net_Value', 'Unit_Profit']
                    # Keep calculated columns near the front (after ASIN and basic details)
                    cols = [c for c in cols if c not in calc_cols]
                    front_cols = cols[:3] if len(cols) >= 3 else cols
                    back_cols = cols[3:] if len(cols) >= 3 else []
                    df_grouped = df_grouped[front_cols + calc_cols + back_cols]
                    df_grouped.to_excel(writer, sheet_name=f"{tab}_result", index=False)

            # 4. Generate Final Aggregated Sheets
            if rankings_data:
                df_rankings = pd.DataFrame(rankings_data).sort_values('Estimated Pallet Profit', ascending=False)
                df_rankings.to_excel(writer, sheet_name='Rankings', index=False)
                
            if summary_results:
                df_summary = pd.DataFrame(summary_results)
                df_summary.to_excel(writer, sheet_name='Summary_Result', index=False)
                
            if distribution_data:
                df_dist = pd.DataFrame(distribution_data)
                
                dist_by_cat = self.distribution_config.get('by_category', True)
                dist_by_catsub = self.distribution_config.get('by_category_subcategory', True)
                dist_by_msrp = self.distribution_config.get('by_msrp', True)
                
                # MSRP Bins
                bins_msrp = [0, 20, 50, 100, 200, float('inf')]
                labels_msrp = ['0-20', '20-50', '50-100', '100-200', '200+']
                df_dist['MSRP_Bin'] = pd.cut(df_dist['Original_MSRP'], bins=bins_msrp, labels=labels_msrp)
                
                if dist_by_cat:
                    df_cat_summary = df_dist.groupby(['File', 'Tab', 'Category']).agg({
                        'Cost': 'sum',
                        'Estimated_Profit': 'sum',
                        'Quantity': 'sum'
                    }).reset_index().sort_values(['File', 'Tab', 'Estimated_Profit'], ascending=[True, True, False])
                    df_cat_summary.to_excel(writer, sheet_name='Dist_ByCategory', index=False)
                    
                if dist_by_catsub:
                    df_catsub_summary = df_dist.groupby(['File', 'Tab', 'Category', 'Subcategory']).agg({
                        'Cost': 'sum',
                        'Estimated_Profit': 'sum',
                        'Quantity': 'sum'
                    }).reset_index().sort_values(['File', 'Tab', 'Estimated_Profit'], ascending=[True, True, False])
                    df_catsub_summary.to_excel(writer, sheet_name='Dist_ByCatSubcat', index=False)
                
                if dist_by_msrp:
                    df_msrp_summary = df_dist.groupby(['File', 'Tab', 'MSRP_Bin'], observed=True).agg({
                        'Cost': 'sum',
                        'Estimated_Profit': 'sum',
                        'Quantity': 'sum'
                    }).reset_index()
                    df_msrp_summary.to_excel(writer, sheet_name='Dist_ByMSRP', index=False)
        
        self.manifest.data["output_files"] = [str(report_path)]
        self.manifest.data["status"] = "completed"
        self.manifest.save()
        logger.info(f"Report generated: {report_path}")
