from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timezone
import os
import json
import uuid

from google.cloud import storage, bigquery

from ingestion.github.extract_pulls import fetch_pull_requests, write_jsonl, utc_today_date_str

from datetime import timedelta

def extract_upload_load(**context):
    token = Variable.get("GITHUB_TOKEN")
    repos = json.loads(Variable.get("GITHUB_REPOS"))

    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_raw = os.environ["GCS_BUCKET_RAW"]
    dataset = os.environ.get("BQ_DATASET_RAW", "gh_raw")
    table_id = f"{project_id}.{dataset}.github_pulls_raw"

    ingestion_date = utc_today_date_str()
    run_id = str(uuid.uuid4())

    # ✅ Incremental cutoff: last 7 days (MVP); later we’ll store cursor in BigQuery/Airflow Variable
    cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    os.makedirs("/opt/airflow/logs/tmp", exist_ok=True)

    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_raw)

    for repo in repos:
        pulls = fetch_pull_requests(token, repo, since_iso=cutoff_dt)

        # ✅ Chunk records to avoid huge files/uploads
        chunk_size = 2000  # adjust: 1000–5000 is reasonable
        chunks = [pulls[i:i+chunk_size] for i in range(0, len(pulls), chunk_size)]

        for idx, chunk in enumerate(chunks):
            local_path = f"/opt/airflow/logs/tmp/{repo.replace('/', '_')}_{ingestion_date}_{run_id}_part{idx}.jsonl"

            with open(local_path, "w", encoding="utf-8") as f:
                for pr in chunk:
                    row = {
                        "ingestion_date": ingestion_date,
                        "repo_full_name": repo,
                        "event_type": "pull",
                        "payload": pr,
                    }
                    f.write(json.dumps(row))
                    f.write("\n")

            gcs_path = (
                f"github/pulls/dt={ingestion_date}/repo={repo.replace('/', '__')}"
                f"/run_id={run_id}/part={idx}/data.jsonl"
            )

            blob = bucket.blob(gcs_path)

            # ✅ Longer timeout + retries
            blob.upload_from_filename(local_path, timeout=300, num_retries=5)

            # ✅ Load each chunk into BigQuery (append)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("ingestion_date", "DATE"),
                    bigquery.SchemaField("repo_full_name", "STRING"),
                    bigquery.SchemaField("event_type", "STRING"),
                    bigquery.SchemaField("payload", "JSON"),
                ],
            )

            load_job = bq_client.load_table_from_uri(
                f"gs://{bucket_raw}/{gcs_path}",
                table_id,
                job_config=job_config,
            )
            load_job.result()

            
with DAG(
    dag_id="github_pulls_ingestion",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["github", "ingestion"],
) as dag:
    run = PythonOperator(
        task_id="extract_upload_load_pulls",
        python_callable=extract_upload_load,
    )