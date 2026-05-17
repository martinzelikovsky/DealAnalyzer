# Keepa Field Mapping Guide

This document explains the various Keepa price and sales rank fields available for use in the `evaluation_config.value_proxy` setting of your `config.yaml` file.

When evaluating the potential value of a wholesale item, Deal Analyzer allows you to choose which Keepa historical metric best represents the "Current Value" of the product on Amazon.

## Available Keepa Value Proxies

Below are the most common proxies mapped from the raw Keepa API fields to their human-readable descriptions. Note that the prefix `keepa_` is prepended to the field name based on your `output_config.enrichment_col_prefix`.

### Current/Latest Prices
*   **`keepa_min`**: The lowest price observed over the historical lookback period (e.g., last 30 days). Useful for a conservative estimate.
*   **`keepa_max`**: The highest price observed over the lookback period.
*   **`keepa_avg`**: The average price observed over the lookback period. Good for a balanced estimate, ignoring temporary dips or spikes.

### Interval-Specific Prices (e.g., 90-day intervals)
*   **`keepa_minInInterval`**: The lowest price recorded in the most recent significant interval (often preferred as a reliable baseline).
*   **`keepa_maxInInterval`**: The highest price in the interval.

### Sales Rank & Demand
While not typically used as a direct price proxy, these are useful for context:
*   **`keepa_salesRank`**: The most recent Amazon Best Sellers Rank. Lower is better.
*   **`keepa_monthlySold`**: Estimated number of units sold per month based on Keepa's data.

## How to Configure

To change how Deal Analyzer calculates the `Estimated_Value`, update your `config.yaml`:

```yaml
evaluation_config:
  value_proxy: 'keepa_avg' # Change this to your preferred metric
  margin_rate: 0.15        # 15% discount for safety margin
```

## Additional Resources
For a complete list of all possible raw fields returned by the Keepa API, refer to the [official Keepa API Documentation](https://keepa.com/#!api).
