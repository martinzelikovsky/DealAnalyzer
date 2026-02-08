# Deal Analyzer

## Objective
This tool analyzes procurement opportunities to provide recommendations to the user.

## How to use

### Setup
TBD

### Triggering a run
TBD

## Appendix

### Proposed Features and Ideas
* The tool can handle multiple excel files in a single triggering command.
* The tool will create result excel files in a directory passed or default directory.
* The tool will support continuing runs that get interrupted, as runs can last for hours.
* The run execution will be configurable via a configuration file, but certain fields may be overridden via CLI. 
* The tool will save the full JSON object passed by the response and timestamp it to prevent unnecessary API calls.

### Requirements
* Input data is in the form of an excel file with multiple tabs for each opportunity.
* `B00 ASIN` which is the Amazon specific ASIN is provided in the data. This tool is limited to Amazon products.
* A [`Keepa`](https://keepa.com/) API key is needed. Execution rate of the tool is limited to the API token rate.
* 
