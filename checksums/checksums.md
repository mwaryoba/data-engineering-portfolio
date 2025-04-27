# Data Quality: Checksum Validation

This module adds basic data integrity checks across the Bronze and Silver layers using SHA-256 checksums.

## Notebooks
- `bronze_layer_checksum.py`
- `silver_layer_checksum.py`
- `compare_layer_checksums.py`

## Utility Script
- `checksum_utils.py`:
  - `detect_data_format()`: Auto-detects CSV, JSON, Delta, or Excel formats.
  - `load_data_with_format()`: Reads data using correct format handler.
  - `generate_checksums()`: Computes SHA-256 checksums by row.
  - `write_checksums()`: Appends to Delta tables and optionally writes to ADLS.
  - `is_df_empty()`: Safely checks if DataFrame is empty.

## Comparison Script
- `compare_layer_checksums.py`: Compares row-level checksums and row counts between layers and prints mismatches.

## Highlights
- Ensures data consistency between Bronze → Silver layers.
- Simple, modular, and extensible design.
- Tracks object-level mismatches and missing data.