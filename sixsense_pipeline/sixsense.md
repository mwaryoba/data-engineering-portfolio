# SixSense Data Pipeline

This project showcases a pipeline built in **Databricks** to extract data from SixSense's SFTP server, apply schema transformations, and load the data into **Snowflake** for analytics and modeling.

---

## Components

### Notebooks
- **`six_sense_intent.py`**: Ingests "Intent" pack files from SFTP.
- **`six_sense_predictiveleads.py`**: Ingests "Predictive Leads" pack files from SFTP.

### Utilities
- **`elt_util.py`**: Shared utility class for Spark transformation, flattening nested structures, and handling ingestion logic.
- **`azure_keyvault.py`**: Handles secure credential retrieval from Azure Key Vault.
- **`six_sense_util.py`**: Manages SFTP connections and file transfers using `paramiko`.

---

## Technologies Used

- **Databricks** (PySpark, Notebook Orchestration)
- **Azure Key Vault** (Secrets Management)
- **Snowflake** (Data Warehouse)
- **SFTP** (Raw file ingestion)
- **Python** (OOP utility classes)

---

## Sample Workflow

1. Connect to SixSense SFTP and list files in `/outgoing`.
2. Match files ending in `intent.csv` or `predictiveleads.csv`.
3. Check Snowflake for previously loaded files to avoid duplication.
4. Pull and load only new files, apply schema transformation, add metadata like `file_name`, and write to: DSV_ELT_INGESTION.SIX_SENSE.RAW_*PACK*