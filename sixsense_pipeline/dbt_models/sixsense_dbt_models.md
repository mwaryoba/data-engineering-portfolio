# dbt Models for SixSense Pipeline

This folder contains the staging and mart layer dbt models used to transform and clean raw SixSense data ingested via Databricks into Snowflake.

---

## Staging Models

- **stg_sixsense_intent.sql**: Cleans and renames columns from the raw `raw_intent` table.
- **stg_sixsense_predictive_leads.sql**: Cleans and renames fields from the `raw_predictiveleads` table.

---

## Mart Models

- **fact_sixsense_intent.sql**: Fact table that models intent activity for accounts.
- **fact_sixsense_predictive_leads.sql**: Fact table representing predicted leads and their scores.

---

## Model Materialization

- Staging models are materialized as **tables**
- Mart models are materialized as **views**

This is configured in `dbt_project.yml`.