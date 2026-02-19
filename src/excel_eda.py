import pandas as pd
import numpy as np
import re
import os
from pathlib import Path

EXCEL_FILE = 'YYZ1 22DEC25.xlsx'
SHEET_NAME_PATTERN = r'Detail_\d+'

def load_excel_sheets_for_eda(excel_file_path, sheet_pattern):
    """
    Loads sheets from an Excel file that match a given pattern into pandas DataFrames.
    Provides an initial overview for exploratory data analysis.
    """
    if not Path(excel_file_path).exists():
        print(f"Error: The file '{excel_file_path}' was not found.")
        return {}

    print(f"Loading Excel file: {excel_file_path}")
    xls = pd.ExcelFile(excel_file_path)
    all_sheet_names = xls.sheet_names
    print(f"All sheets found: {all_sheet_names}")

    filtered_sheet_names = [sheet for sheet in all_sheet_names if re.match(sheet_pattern, sheet)]
    print(f"Sheets matching pattern '{sheet_pattern}': {filtered_sheet_names}")

    if not filtered_sheet_names:
        print(f"No sheets found matching the pattern '{sheet_pattern}'.")
        return {}

    dataframes = {}
    for sheet_name in filtered_sheet_names:
        print(f"Loading sheet: {sheet_name}")
        try:
            df = xls.parse(sheet_name)
            dataframes[sheet_name] = df
            print(f"Successfully loaded sheet '{sheet_name}' into a DataFrame.")
            print(f"--- Info for {sheet_name} ---")
            df.info()
            print(f"--- Head for {sheet_name} ---")
            print(df.head())
            print("\n")
        except Exception as e:
            print(f"Error loading sheet '{sheet_name}': {e}")
            continue
    return dataframes

if __name__ == "__main__":
    print("This script will load specified sheets from the Excel file into pandas DataFrames.")
    print("It will then print initial info and head for each DataFrame to facilitate EDA.")
    print("To install required libraries, run: pip install pandas openpyxl")
    print("\n")

    loaded_data = load_excel_sheets_for_eda(EXCEL_FILE, SHEET_NAME_PATTERN)

    if loaded_data:
        print("Data loading complete. You can now access your DataFrames in the 'loaded_data' dictionary.")
        print("For example, to access the DataFrame for 'Detail_1': loaded_data['Detail_1']")
        print("\nTo perform interactive EDA, you can run this script in an IPython/Jupyter environment,")
        print("or use a Python debugger (e.g., by setting a breakpoint at the end of the script).")
    else:
        print("No data was loaded. Please check the Excel file name and sheet patterns.")
    
    msrp_bins = [0,5,10,20,30,50,75,100,250,500,1000,2000,3500, 5000, 7500, 10000, 15000]
    df = loaded_data.get('Detail_1')
    if df is not None:
        df['msrp_bin'] = pd.cut(df['MSRP'], bins=msrp_bins)

        description_df = df.groupby('Category').agg(
            msrp_sum=('EXT MSRP', 'sum'),
            quantity_sum=('Quantity', 'sum'),
            asin_count=('B00 ASIN', 'nunique'),
        )
        description_df['cost_perc'] = 100 * description_df['msrp_sum'] / df['EXT MSRP'].sum()
        description_df['quantity_perc'] = 100 * description_df['quantity_sum'] / df['Quantity'].sum()
        description_df.sort_values(by='cost_perc', ascending=False, inplace=True)

        description_df2 = df.groupby(['Category', 'Sub-Category']).agg(
            msrp_sum=('EXT MSRP', 'sum'),
            quantity_sum=('Quantity', 'sum'),
            asin_count=('B00 ASIN', 'nunique'),
        )
        description_df2['cost_perc'] = 100 * description_df2['msrp_sum'] / df['EXT MSRP'].sum()
        description_df2['quantity_perc'] = 100 * description_df2['quantity_sum'] / df['Quantity'].sum()
        description_df2.sort_values(by='cost_perc', ascending=False, inplace=True)

        description_df3 = df.groupby('msrp_bin').agg(
            msrp_sum=('EXT MSRP', 'sum'),
            quantity_sum=('Quantity', 'sum'),
            asin_count=('B00 ASIN', 'nunique'),
        )
        description_df3 = description_df3[description_df3['quantity_sum'] > 0].sort_values(by='msrp_bin')
        description_df3['cost_perc'] = 100 * description_df3['msrp_sum'] / df['EXT MSRP'].sum()
        description_df3['quantity_perc'] = 100 * description_df3['quantity_sum'] / df['Quantity'].sum()

        with pd.ExcelWriter('high_level_analysis.xlsx') as writer:
            description_df.to_excel(writer, sheet_name='Grouping by Category')
            description_df2.to_excel(writer, sheet_name='Grouping by Sub-Category')
            description_df3.to_excel(writer, sheet_name='GRouping by MSRP bin')
