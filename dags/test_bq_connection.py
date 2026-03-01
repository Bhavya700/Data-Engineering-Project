from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id="test_bigquery_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_query = BigQueryInsertJobOperator(
        task_id="run_test_query",
        configuration={
            "query": {
                "query": "SELECT 1 as test_col",
                "useLegacySql": False,
            }
        },
        location="us-west1",
    )