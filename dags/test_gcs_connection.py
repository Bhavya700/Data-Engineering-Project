from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime

with DAG(
    dag_id="test_gcs_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    list_files = GCSListObjectsOperator(
        task_id="list_bucket",
        bucket="{{ var.value.GCS_BUCKET_RAW }}",
    )