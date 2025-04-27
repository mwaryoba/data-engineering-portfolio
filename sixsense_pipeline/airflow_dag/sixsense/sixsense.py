from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from plugins.slack.slack_operator import SlackNotification
from utilities.config import get_config

config, environment = get_config()
slack_alert = SlackNotification(environment)

DATABRICKS_CONN_ID = 'azure-databricks-uri'
DBT_CLOUD_CONN_ID = 'dbt-cloud-uri'

default_args = {
    'owner': 'ServiceTitan Airflow',
    'retries': 1,
    'on_failure_callback': slack_alert.slack_fail_notification,
}

with DAG(
    dag_id='sixsense_intent_predictiveleads',
    schedule_interval='0 23 * * *',
    start_date=datetime(2024, 1, 10),
    max_active_runs=1,
    catchup=False,
    tags=['sixsense', 'databricks', 'dbt'],
    dagrun_timeout=timedelta(hours=3),
    default_args=default_args,
) as dag:

    databricks_intent = DatabricksRunNowOperator(
        task_id="databricks_sixsense_intent",
        job_id=config[environment]['six_sense']['databricks']['six_sense_intent'],
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={"env": environment},
        execution_timeout=timedelta(hours=2)
    )

    databricks_predictiveleads = DatabricksRunNowOperator(
        task_id="databricks_sixsense_predictiveleads",
        job_id=config[environment]['six_sense']['databricks']['six_sense_predictiveleads'],
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={"env": environment},
        execution_timeout=timedelta(hours=2)
    )

    dbt_intent = DbtCloudRunJobOperator(
        task_id='dbt_sixsense_intent',
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=config[environment]['six_sense']['dbt']['job_intent_dbt'],
        execution_timeout=timedelta(minutes=59),
    )

    dbt_predictiveleads = DbtCloudRunJobOperator(
        task_id='dbt_sixsense_predictiveleads',
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=config[environment]['six_sense']['dbt']['job_predictiveleads_dbt'],
        execution_timeout=timedelta(minutes=59),
    )

    databricks_intent >> dbt_intent
    databricks_predictiveleads >> dbt_predictiveleads