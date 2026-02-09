# ETL-and-ELT-in-Python

Data pipelines are at the foundation of every strong data platform. Building these pipelines is an essential skill for data engineers, who provide incredible value to a business ready to step into a data-driven future.

---

## Overview

This project provides a comprehensive **ETL (Extract, Transform, Load)** pipeline implementation in Python for processing educational scoring data. The pipeline supports multiple data formats and includes utilities for data extraction, transformation, and loading.

---

## Features

### Extraction
- **extract_from_csv()** - Load data from CSV files
- **extract_from_parquet()** - Load data from Parquet files using fastparquet engine
- **extract_from_json()** - Load data from JSON files with index-oriented structure
- **json_to_df()** - Convert raw JSON file to normalized DataFrame with extracted school information and scores

### Transformation
- **transform()** - Convert percentage columns to float format and clean data, selecting specific school columns
- **transform_df_json_based()** - Flatten nested 'scores' column into individual columns and handle null values

### Loading
- **load_to_csv()** - Save transformed data to CSV files
- **load_to_parquet()** - Save transformed data to Parquet files
- **load_to_db()** - Load DataFrames into PostgreSQL database tables

---

## Installation

### Requirements
Install dependencies from `requirements.txt`:

```bash
conda install --file requirements.txt
```

### Dependencies
- pandas
- sqlalchemy
- fastparquet
- pytest (for running tests)

---

## Quick Start

### Running the Complete Pipeline

The main pipeline (`etl_pipe.py`) executes two concurrent ETL workflows:

#### Pipeline 1: Parquet to CSV
```python
from etl_utils import extract_from_parquet, transform, load_to_csv

# Extract data from Parquet
extracted_data = extract_from_parquet("data-sources/scores.parquet")

# Transform percentage columns to float
clean_data = transform(extracted_data)

# Load to CSV
load_to_csv(clean_data, "data-sources/scores_cleaned.csv")
```

#### Pipeline 2: JSON to CSV
```python
from etl_utils import extract_from_json, transform_df_json_based, load_to_csv

# Extract from JSON
df_json = extract_from_json("data-sources/testing_scores.json")

# Transform and flatten nested scores
df_clean = transform_df_json_based(df_json)

# Load to CSV
load_to_csv(df_clean, "data-sources/testing_scores.csv")
```

### Running the Pipeline

Execute the main pipeline script:

```bash
python etl_pipe.py
```

This will:
1. Extract data from Parquet and transform it to CSV (with logging)
2. Wait 10 seconds
3. Extract data from JSON and transform it to CSV (with logging)
4. Generate pipeline execution logs in `pipeline.log`

---

## Data Sources

The `data-sources/` directory contains sample datasets for ETL processing:

- **scores.csv** - Raw education scoring data with percentage columns (demographic breakdowns)
- **scores.parquet** - Same scoring data in Parquet format for efficient processing
- **scores_cleaned.csv** - Processed output from Pipeline 1 with transformed percentage columns
- **testing_scores.json** - Student test scores in JSON format with nested score structures (math, reading, writing)
- **testing_scores.csv** - Processed output from Pipeline 2 with flattened scores data

---

## Project Structure

```
Data-Pipelines/
├── etl_pipe.py               # Main pipeline orchestration (runs both ETL workflows)
├── etl_utils.py              # Core ETL utility functions (extract, transform, load)
├── requirements.txt          # Project dependencies
├── README.md                 # This file
├── pipeline.log              # Execution logs from pipeline runs
├── tests/                    # Unit tests
│   └── unit_test.py         # Test cases for extraction and transformation functions
└── data-sources/             # Sample datasets
    ├── scores.csv            # Raw scoring data (CSV)
    ├── scores.parquet        # Raw scoring data (Parquet format)
    ├── scores_cleaned.csv    # Processed scores from Pipeline 1
    ├── testing_scores.json   # Raw test scores (JSON format)
    └── testing_scores.csv    # Processed scores from Pipeline 2
```

---

## Usage

### Run the Complete Pipeline

Execute both ETL workflows with logging:

```bash
python etl_pipe.py
```

This runs:
1. **Pipeline 1**: Parquet file → Clean percentages → Save as CSV
2. **Pipeline 2**: JSON file → Flatten nested scores → Save as CSV

Execution details are logged to `pipeline.log`.

### Import Functions in Your Own Code

```python
from etl_utils import (
    extract_from_csv,
    extract_from_parquet,
    extract_from_json,
    transform,
    transform_df_json_based,
    load_to_csv,
    load_to_parquet,
    load_to_db
)

# Use functions to build custom pipelines
```

### Test Individual Functions

```bash
# Test the JSON to DataFrame conversion
python etl_utils.py
```

### Run Unit Tests

Execute the test suite using pytest:

```bash
python -m pytest tests/unit_test.py 
```

Or run tests with verbose output:

```bash
python -m pytest tests/unit_test.py -v
```

The test suite includes:
- **test_extract_csv()** - Validates CSV extraction returns a pandas DataFrame
- **test_extract_parquet()** - Validates Parquet extraction returns a pandas DataFrame
- **test_extract_json()** - Validates JSON extraction returns a pandas DataFrame
- **test_transform_df_json_based()** - Validates JSON transformation produces a DataFrame with correct structure

---

## Notes

- The pipeline executes two separate workflows with a 10-second delay between them
- **Pipeline 1**: Handles school demographic data with percentage columns (percent Black, Hispanic, Asian, Tested)
- **Pipeline 2**: Handles student test scores in JSON format with nested structure
- Missing values in scores are filled with the mean value rounded to the nearest integer
- Percentage columns are automatically stripped of '%' symbols and converted to float during transformation
- All extraction functions return Pandas DataFrames for flexible downstream processing
- Error handling is implemented with try-except blocks and detailed logging
- Logging includes DataFrame shapes before and after transformations for data quality tracking
- Supports PostgreSQL database loading via SQLAlchemy (connection URL required)