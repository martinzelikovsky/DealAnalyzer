# KeepaAPI Client Design - Focusing on `Keepa.query` with Caching and DataFrame Output

## 1. Purpose
The `KeepaAPI` class will serve as a dedicated client for interacting with the Keepa API, specifically leveraging the `keepa.Keepa.query` function from the Python SDK. Its primary goal is to efficiently retrieve product data, including price history and sales rank, for specified ASINs, incorporating a local caching mechanism to reduce API calls and improve performance. It will also provide a structured output as a Pandas DataFrame.

## 2. Core Functionalities (via `keepa.Keepa.query`)
*   **Initialization**: Initialize the `Keepa` object from the SDK with the API key and desired domain.
*   **Product Query**: Make requests to the Keepa API for product data using ASINs.
*   **Data Extraction**: Extract relevant price, sales rank, and other product details from the `keepa.Keepa.query` response.
*   **Rate Limit Handling**: The underlying Keepa SDK is expected to handle rate limiting internally. The client will rely on the SDK's built-in mechanisms for this.
*   **Error Management**: Propagate or wrap exceptions raised by the Keepa SDK for clearer error handling.
*   **Caching**: Implement a local file-based caching mechanism to store the *entire raw JSON response* from `keepa.Keepa.query` for a configurable duration. Caching can also be enabled/disabled.
*   **DataFrame Output**: Provide a function to convert the processed API response into a Pandas DataFrame, including specific columns defined in the `enrichment_cols` section of the configuration.

## 3. Key Methods and Responsibilities

### `__init__(self, api_key: str, domain: int, cache_max_age_days: int = 7, enable_cache: bool = True, config_enrichment_cols: list[str] = None)`
*   **Responsibility**: Initializes the `KeepaAPI` client and the underlying `keepa.Keepa` SDK object.
*   **Parameters**:
    *   `api_key` (str): Your private Keepa API access key.
    *   `domain` (int): The Keepa domain ID to target (e.g., `1` for DE, `2` for US, `3` for UK, `4` for CA). This is an integer value as used by the `keepa` library.
    *   `cache_max_age_days` (int): The maximum age (in days) for a cached ASIN's data to be considered valid.
    *   `enable_cache` (bool): A flag to enable or disable the caching mechanism.
    *   `config_enrichment_cols` (list[str]): List of column names to extract from API response, as defined in `config.yaml`.
*   **Action**: Stores the API key, domain, cache settings, and enrichment columns. Instantiates the `keepa.Keepa` object and sets up the cache directory.

### `_get_cache_path(self, asin: str) -> str`
*   **Responsibility**: Generates the full file path for a given ASIN's cache file.
*   **Parameters**:
    *   `asin` (str): The ASIN for which to generate the cache path.
*   **Action**: Constructs the path `cache/{asin}_{YYYY-MM-DD}.json` within the project root, where `YYYY-MM-DD` is the current UTC date.
*   **Returns**: The absolute path to the potential cache file.

### `_read_from_cache(self, asin: str) -> dict | None`
*   **Responsibility**: Checks if a valid, unexpired cache entry exists for an ASIN and reads it.
*   **Parameters**:
    *   `asin` (str): The ASIN to check in the cache.
*   **Action**:
    *   Constructs the expected cache file path.
    *   Checks if the file exists, if caching is enabled, and if its modification time is within `cache_max_age_days`.
    *   If valid, reads the JSON content and returns it.
*   **Returns**: The cached JSON data as a dictionary if valid and found, otherwise `None`.

### `_write_to_cache(self, asin: str, data: dict) -> None`
*   **Responsibility**: Writes the raw JSON response for an ASIN to the cache.
*   **Parameters**:
    *   `asin` (str): The ASIN to cache.
    *   `data` (dict): The raw JSON response from `keepa.Keepa.query`.
*   **Action**:
    *   Creates the cache directory if it doesn't exist.
    *   Writes the `data` to the cache file using the format `cache/{asin}_{YYYY-MM-DD}.json`.

### `get_product_data(self, asins: list[str], stats: int = None, history: bool = True) -> list[dict]`
*   **Responsibility**: Fetches detailed product information, including price and sales rank history, for a list of ASINs, prioritizing cached data.
*   **Parameters**:
    *   `asins` (list[str]): A list of ASINs to retrieve product data for.
    *   `stats` (int, optional): An integer representing the number of days for which to calculate basic statistics (e.g., 30 for 30-day average). Defaults to `None`.
    *   `history` (bool, optional): If `True`, retrieves full price and sales rank history. Defaults to `True`.
*   **Action**:
    *   For each ASIN:
        1.  If `enable_cache` is `True`, attempt to `_read_from_cache`.
        2.  If cached data is valid, use it.
        3.  If not in cache or expired, call `keepa.Keepa.query`.
        4.  On successful API call, if `enable_cache` is `True`, `_write_to_cache` the *entire raw response*.
    *   Processes the raw (cached or fresh) response to extract relevant product details, price history, and sales rank history.
    *   Handles potential `KeepaAPIException` from the SDK.
*   **Returns**: A list of dictionaries, where each dictionary represents a product and its attributes, including processed historical data.

### `get_results_dataframe(self, product_data: list[dict]) -> pandas.DataFrame`
*   **Responsibility**: Converts a list of processed product data dictionaries into a Pandas DataFrame, selecting and typing columns as specified in the configuration.
*   **Parameters**:
    *   `product_data` (list[dict]): A list of dictionaries, each representing a product's data after processing.
*   **Action**:
    *   Creates a Pandas DataFrame from the `product_data`.
    *   Selects only the columns specified in `config_enrichment_cols`.
    *   Applies appropriate Pandas data types to the columns based on a predefined mapping.
*   **Returns**: A `pandas.DataFrame` containing the requested product information.

## 4. Configuration and Parameters
*   `api_key` (str): Obtained from the Keepa API console. Passed directly to `keepa.Keepa`.
*   `domain` (int): Keepa domain ID (e.g., `1` for DE, `2` for US). Passed directly to `keepa.Keepa`.
*   `cache_max_age_days` (int): Configurable in `config.yaml` (e.g., `7` days). This value will be passed during `KeepaAPI` initialization.
*   `enable_cache` (bool): Configurable in `config.yaml` (e.g., `True` or `False`). This flag will control whether caching is active.
*   `enrichment_cols`: Defined in `config.yaml`. These columns will be used by `get_results_dataframe`.
*   **Rate Limiting**: Handled implicitly by the `keepa` SDK. The client won't explicitly manage rate limits but will be aware of potential delays or errors from the SDK.

## 5. `config.yaml` as a Template
`config.yaml` at the project root will serve as a template. For testing purposes, `configs/sample_config.yaml` will be used to define specific configurations.

## 6. Mapping Output Columns to Pandas Data Types
A mapping will be established within the `KeepaAPI` class (or a helper utility) to ensure that columns extracted into the Pandas DataFrame have the correct data types. This will involve a dictionary-like structure where keys are column names (or patterns) and values are Pandas-compatible data types (e.g., `int`, `float`, `datetime`, `str`).

## 7. Follow-up Questions
1.  **Cache Directory Location**: Is `cache/{asin}_{YYYY-MM-DD}.json` at the project root the preferred location and naming convention for cache files, or should the `cache` directory path itself be configurable in `config.yaml`?
    *   **Project Root**: Keep `cache/` at the project root.
    *   **Configurable Path**: Allow the cache directory to be specified in `config.yaml`.
2.  **Specific Data Types**: For the `enrichment_cols` defined in `config.yaml`, what are the exact Pandas data types you expect for each? Providing specific types will help create an accurate mapping.
    *   Example: `title`: `str`, `min_price`: `float`, `sales_rank`: `int`.

Once this updated plan is reviewed and approved, I can proceed with implementing the core functionality of the `KeepaAPI` class in `code` mode.

```mermaid
graph TD
    A[Start] --> B(Initialize KeepaAPI with API Key, Domain ID, Cache Settings, Enrichment Cols)
    B --> C{Call get_product_data for ASINs?}
    C -- Yes --> D{Is Caching Enabled?}
    D -- No --> F(Call keepa.Keepa.query from SDK)
    D -- Yes --> E(Check Cache for ASIN)
    E -- Cache Hit & Valid --> K(Process Cached Data)
    E -- Cache Miss or Expired --> F
    F --> G{SDK Response Received?}
    G -- No --> H(Handle SDK Exception/Error)
    G -- Yes --> I{Is Caching Enabled?}
    I -- Yes --> J(Write Raw Response to Cache)
    J --> K
    K --> L(Extract and Process Product Data)
    L --> M(Create Pandas DataFrame from Processed Data)
    M --> N(Return DataFrame)
    C -- No --> O[End]