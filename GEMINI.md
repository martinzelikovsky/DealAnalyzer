# Deal Analyzer Project Context

## Project Overview
**Deal Analyzer** is a Python-based CLI tool designed to automate the analysis of Amazon product procurement opportunities. It reads input data from Excel files (containing ASINs), enriches it with real-time and historical market data using the [Keepa API](https://keepa.com/), and generates comprehensive reports.

The project recently underwent an architectural refactor to improve reliability for long-running processes by introducing a **Manifest-based State System** and a **Staging Area** for intermediate results.

## Key Features
- **Excel Input/Output:** Processes specific tabs (regex-matched) from multiple Excel files.
- **Keepa Integration:** Fetches price history, sales rank, and category data.
- **Smart Caching:** Caches API responses locally (default 7 days) to minimize API token usage.
- **Resumability:** Tracks progress via `state.json`. If interrupted, it resumes exactly from the last processed ASIN.
- **Atomic Writes:** Uses a staging directory for intermediate CSVs to prevent data corruption before finalizing the main Excel report.

## Directory Structure
```
/deal_analyzer
├── run.py               # Application entry point wrapper
├── src/                 # Source code directory
│   ├── main.py          # Core CLI entry point & argument parsing
│   ├── deal_analyzer.py # Manifest management, orchestrating processing
│   ├── keepa_client.py  # Keepa API wrapper with caching logic
│   ├── utils.py         # Logging & helper utilities
│   └── excel_eda.py     # Standalone script for exploratory data analysis
├── config.yaml          # Template configuration file
├── requirements.txt     # Python dependencies
├── cache/               # Directory for cached Keepa Pickle responses
├── results/             # Directory for output reports and run staging
│   └── <Run_Name>/      # Unique directory per run
│       ├── state.json   # Manifest file tracking progress
│       └── staging/     # Intermediate CSV files
└── tests/               # Unit tests (Pytest)
```

## Setup & Installation

1.  **Dependencies:**
    The project uses `uv` for dependency and environment management.
    ```bash
    uv sync
    ```
2.  **Environment Variables:**
    The tool requires a valid Keepa API key.
    ```bash
    export KEEPA_KEY='your_keepa_api_key'
    ```

## Usage

### Basic Execution
Run with default settings (reads `config.yaml` and processes inputs from `~/deal_analyzer_input`):
```bash
uv run python run.py
```

### Custom Configuration
You can override specific parameters via CLI arguments or provide a separate config file:
```bash
uv run python run.py --config configs/my_custom_config.yaml
# OR
uv run python run.py --lookback_days 90 --domain US
```

### Configuration (`config.yaml`)
The configuration file controls:
- **Input/Output Paths:** Where to find Excel files and save results.
- **Tab Selection:** Regex to identify which Excel sheets to process (e.g., `Detail_\d+`).
- **Enrichment:** Which specific data points (columns) to extract from Keepa (e.g., `sales_rank`, `min_price`).

## Architecture & Workflows

### 1. Initialization
- `run.py` invokes `src/main.py` which parses arguments and sets up the `KeepaAPI`.
- A unique run directory is created in `results/` based on input filenames.
- `state.json` (Manifest) is loaded or created to track the run's state.

### 2. Processing (Staging Phase)
- `DealAnalyzer` iterates through input files and tabs.
- **Manifest Check:** It checks `state.json` to see if a tab is already completed.
- **Staging:** As ASINs are processed, results are saved in chunks to `results/<Run_Name>/staging/<File>_<Tab>.csv`.
- **Resume Logic:** If a run restarts, it reads the staging CSV to determine the last processed ASIN and resumes fetching from there.

### 3. Finalization
- Once all tabs are processed, the `finalize()` method is called.
- It reads all CSVs from the `staging/` directory.
- It compiles them into a single `_result.xlsx` workbook with formatted sheets.
- The manifest is updated to mark the run as `completed`.

## Development Conventions
- **Source Layout:** All core logic resides in the `src/` directory.
- **Path Handling:** All file paths are handled using `pathlib.Path` for cross-platform compatibility.
- **Logging:** Centralized logging via `utils.config_logger`.
- **Testing:** Tests are located in `tests/` and use `pytest` with `pytest-mock`.
- **Typing:** Type hints are used where possible for clarity.

## Key Commands
- **Run Tests:** `uv run pytest`
- **Clean Cache:** `rm -rf cache/*` (forces fresh API calls)

In [4]: excel.sheet_names
Out[4]: 
['Summary',
 'Detail_1',
 'Detail_2',
 'Detail_3',
 'Detail_4',
 'Detail_5',
 'Detail_6',
 'Detail_7',
 'Manifest',
 'Images 1']

In [5]: excel.parse('Summary')
Out[5]: 
                FOB:         Brampton LSI Unnamed: 2   Unnamed: 3       Unnamed: 4       Unnamed: 5 Unnamed: 6   Unnamed: 7 Unnamed: 8
0              Date:  2025-12-19 00:00:00        NaN          NaN              NaN              NaN        NaN          NaN        NaN
1                NaN                  NaN        NaN          NaN              NaN              NaN        NaN          NaN        NaN
2   Manifest Summary                  NaN        NaN          NaN              NaN              NaN        NaN          NaN        NaN
3             Detail                 Lane    Pallets          NaN  Sum of Quantity  Sum of EXT MSRP     MSRP %     FOB Cost        NaN
4                  1                 YYZ1         24     20776195             8104        395651.31      0.142  56182.48602        NaN
5                  2                 YYZ1         23     20778731             8019        388879.61      0.142  55220.90462        NaN
6                  3                 YYZ1         24     20781844             7084        317892.87      0.142  45140.78754        NaN
7                  4                 YYZ1         24     20781874            11144        530673.02      0.142  75355.56884        NaN
8                  5                 YYZ1         23     20785449             9227        378978.26      0.142  53814.91292        NaN
9                  6                 YYZ9         23     20785452              756        118489.17      0.186  22038.98562       sold
10                 7                 YYZ9         24     20785454              488         74066.21      0.186  13776.31506       sold
11               NaN                  NaN        NaN  Grand Total            44822       2204630.45        NaN          NaN        NaN          NaN        NaN