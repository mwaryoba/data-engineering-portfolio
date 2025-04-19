# Data Engineering Portfolio

Welcome to my data engineering portfolio! This repository highlights real-world projects I've worked on using tools such as **Databricks**, **dbt**, **Airflow**, and **Azure Data Lake**. Each project is built around data engineering workflows with reusable components and documentation.

---

## Projects Included

### 1. **SixSense Data Pipeline (SFTP Data Ingestion)**
A full pipeline that ingests multiple data pack files with varying schemas from an SFTP server, transforms the data using dbt, and orchestrates the jobs with Airflow.

- **Extraction**: PySpark script in Databricks to dynamically parse and flatten different file formats.
- **Transformation**: dbt models for staging and mart layers.
- **Orchestration**: Airflow DAG to automate the extraction, transformation, and loading steps.

➡️ See: `sixsense_pipeline/`

---

### 2. **Checksum Validation for Data Integrity**
Implements a strategy to validate row-level data consistency across the Bronze and Silver layers using SHA-256 checksums. Built-in logging and error reporting for mismatches.

➡️ See: `checksum/`

---

### 3. **API Data Ingestion**
Scripts to pull raw data from third-party APIs like Pendo and Payrix, normalize it, and save it into Azure Data Lake as Delta tables.

➡️ See: `pendo_api_ingestion/`, `jira_api_ingestion/`

---

## Technologies Used

- **Databricks** (PySpark, Notebooks)
- **dbt** (data transformation modeling)
- **Apache Airflow** (ETL orchestration)
- **Azure Data Lake** (cloud storage)
- **Snowflake** (data warehouse integration)
- **Python**, **SQL**, **Jinja**

---

## Contact

**Michael Waryoba**  
Dunwoody, GA  
[mwaryoba19@gmail.com](mailto:mwaryoba19@gmail.com)  
[LinkedIn Profile](https://www.linkedin.com/in/michael-waryoba-620207a4/)