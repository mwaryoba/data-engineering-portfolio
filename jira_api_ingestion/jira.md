# Jira API Ingestion

This module ingests data from the Jira Cloud API for issue tracking and board metadata.

## Ingested Objects
- `RAW_JIRA_BOARD`
- `RAW_JIRA_ISSUE_ACTIVE`

## Components
- **JiraUtil**: Authenticates and abstracts API interactions (pagination, SLA data, etc.)
- **Databricks Notebooks**:
  - `jira_board`: Ingests Jira board metadata.
  - `jira_issue_active`: Pulls active Jira issues with pagination.

## Highlights
- Secure API auth using Azure Key Vault.
- Supports pagination and historical extract timestamps.
- Writes results directly to Snowflake staging schemas.