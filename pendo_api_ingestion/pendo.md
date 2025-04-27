# Pendo API Ingestion

This module ingests data from the Pendo API into Snowflake using Databricks notebooks.

## Ingested Objects
- `raw_pendo_account`
- `raw_pendo_visitor`
- `raw_pendo_customer_portal_account`
- `raw_pendo_customer_portal_visitor`

## Components
- **PendoApiUtil**: Utility class to authenticate, build payloads, and send requests to Pendo's API.
- **Databricks Notebooks**:
  - `pendo_account`: Pulls account-level metadata.
  - `pendo_visitor`: Pulls visitor data since the last extract timestamp.

## Highlights
- Uses `AzureKeyVault` for secure credential management.
- Ingests data into Snowflake using `SnowflakeUtil`.
- Supports both standard and customer portal subscriptions.