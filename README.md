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

### Transformation
- **transform()** - Convert percentage columns to float format and clean data
- **transform_df_json_based()** - Flatten nested JSON structures and restructure dataframes

### Loading
- **load_to_csv()** - Save transformed data to CSV files
- **load_to_parquet()** - Save transformed data to Parquet files

---

## Installation

### Requirements
Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

### Dependencies
- pandas
- fastparquet

---

## Quick Start

### Example: Processing JSON Scores Data

```python
from etl_utils import extract_from_json, transform_df_json_based, load_to_csv

# Step 1: Extract data from JSON
filepath = 'data-sources/testing_scores.json'
df = extract_from_json(filepath)

# Step 2: Transform the data
clean_data = transform_df_json_based(df)

# Step 3: Load to CSV
load_to_csv(clean_data, 'output/processed_scores.csv')
```

### Example: Processing CSV Scores Data

```python
from etl_utils import extract_from_csv, transform, load_to_parquet

# Step 1: Extract
data = extract_from_csv('data-sources/scores.csv')

# Step 2: Transform percentages to float
cleaned = transform(data)

# Step 3: Load to Parquet
load_to_parquet(cleaned, 'output/scores_output.parquet')
```

---

## Data Sources

The `data-sources/` directory contains sample datasets:

- `scores.csv` - Raw scoring data with percentage columns
- `scores_cleaned_v1.csv` through `scores_cleaned_v5.csv` - Various cleaned versions
- `testing_scores.json` - JSON format with nested scores structure

---

## Project Structure

```
Data-Pipelines/
├── etl_utils.py              # Core ETL utility functions
├── etl_pipe.py               # Main pipeline orchestration
├── requirements.txt          # Project dependencies
├── README.md                 # This file
└── data-sources/             # Sample datasets
    ├── scores.csv
    ├── testing_scores.json
    └── scores_cleaned_v*.csv
```

---

## Usage

Run the pipeline example:

```bash
python etl_utils.py
```

Or import functions into your own pipeline:

```bash
python etl_pipe.py
```

---

## Notes

- The pipeline handles missing values by filling them with 0
- Percentage columns are automatically stripped of '%' symbols during transformation
- All extraction functions return Pandas DataFrames for flexible downstream processing