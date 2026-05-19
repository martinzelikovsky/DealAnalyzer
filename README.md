# Deal Analyzer

## Objective
**Deal Analyzer** is a Python-based CLI tool designed to automate the analysis of Amazon product procurement opportunities. It reads input data from Excel files, enriches it with real-time and historical market data using the Keepa API, and generates comprehensive reports and an interactive Streamlit dashboard to help visualize the potential profit and ROI of different procurement options.

## Key Features
- **Excel Input/Output:** Processes multiple tabs from Excel files and generates a compiled results workbook.
- **Keepa Integration:** Fetches price history, sales rank, and category data.
- **Smart Caching:** Caches API responses locally to minimize API token usage.
- **Resumability:** Tracks progress via a manifest (`state.json`). If interrupted, it resumes exactly from the last processed ASIN.
- **Interactive Dashboard:** Includes a local Streamlit web application to visually explore category distributions, MSRP ranges, ROI, and estimated profits with dynamic drill-down capabilities.

## Prerequisites
- A [Keepa API](https://keepa.com/) key is required to fetch product data.
- macOS, Linux, or Windows (WSL recommended).

## Installation

This project uses **[uv](https://github.com/astral-sh/uv)**, an extremely fast Python package and project manager written in Rust, to manage dependencies and execution.

### 1. Install `uv`
If you do not have `uv` installed, install it using the official script:

**macOS and Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Install Project Dependencies
Navigate to the project directory and use `uv` to sync the dependencies. This will automatically create a virtual environment and install all required packages.

```bash
cd deal_analyzer
uv sync
```

## Configuration

1. **Keepa API Key:** Export your Keepa API key as an environment variable before running the tool:
   ```bash
   export KEEPA_KEY='your_keepa_api_key_here'
   ```

2. **Run Configuration (`config.yaml`):** The tool relies on a configuration file (default `config.yaml`) to know where to look for input files and how to process them. You can customize the input directories, default evaluation metrics, and distribution grouping logic here.

## How to Use

### 1. Triggering a Run (Pipeline)
To process your input Excel files and generate the enriched results, run the main pipeline script. The tool will read the configuration, iterate through the specified Excel files and tabs, fetch Keepa data, and output a `_result.xlsx` file in the `results/` folder.

```bash
uv run python run.py
```
*Note: Since the process can take a long time, the tool saves its progress in a `staging/` directory. If you cancel the script or it crashes, simply run the command again and it will pick up right where it left off!*

### 2. Viewing the Interactive Dashboard
Once the pipeline has finished and generated the `_result.xlsx` file, you can launch the interactive Streamlit dashboard to analyze the outcomes visually.

```bash
uv run streamlit run dashboard.py
```

This will start a local web server and open the dashboard in your default web browser (typically at `http://localhost:8501`).
From the dashboard, you can:
- Select which run to view.
- Analyze Pallet ROI and Enrichment Rates.
- Dynamically drill down into the Keepa category tree.
- Filter and view price distribution and profit contribution based on Original MSRP or Estimated Sale Price.

## Development Notes
- The tool uses caching to prevent redundant Keepa API calls. If you need to force fresh data, you can clear the `cache/` directory.
- Test suites can be executed via `uv run pytest`.
