from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timezone
import os, json, uuid

from google.cloud import storage, bigquery

from ingestion.github.cursor import get_cursor, upsert_cursor
from ingestion.github.extract_pr_reviews import fetch_pr_reviews_incremental

def pr_reviews_extract_upload_load():
    token = Variable.get("GITHUB_TOKEN")
    repos = json.loads(Variable.get("GITHUB_REPOS"))

    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_raw = os.environ["GCS_BUCKET_RAW"]
    dataset = os.environ.get("BQ_DATASET_RAW", "gh_raw")
    table_id = f"{project_id}.{dataset}.github_pr_reviews_raw"

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = str(uuid.uuid4())
    os.makedirs("/opt/airflow/logs/tmp", exist_ok=True)

    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_raw)

    for repo in repos:
        since_ts = get_cursor(bq_client, project_id, dataset, "pr_reviews", repo, fallback_days=7)

        rows, max_pr_updated_at = fetch_pr_reviews_incremental(token, repo, since_ts)

        # Chunk rows
        chunk_size = 2000
        chunks = [rows[i:i+chunk_size] for i in range(0, len(rows), chunk_size)]

        for idx, chunk in enumerate(chunks):
            local_path = f"/opt/airflow/logs/tmp/{repo.replace('/', '_')}_{ingestion_date}_{run_id}_reviews_part{idx}.jsonl"

            with open(local_path, "w", encoding="utf-8") as f:
                for item in chunk:
                    pr_number = item["pr_number"]
                    review = item["review"]
                    row = {
                        "ingestion_date": ingestion_date,
                        "repo_full_name": repo,
                        "pr_number": pr_number,
                        "event_type": "pr_review",
                        "payload": review,
                    }
                    f.write(json.dumps(row)); f.write("\n")

            gcs_path = f"github/pr_reviews/dt={ingestion_date}/repo={repo.replace('/', '__')}/run_id={run_id}/part={idx}/data.jsonl"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path, timeout=300, num_retries=5)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("ingestion_date", "DATE"),
                    bigquery.SchemaField("repo_full_name", "STRING"),
                    bigquery.SchemaField("pr_number", "INT64"),
                    bigquery.SchemaField("event_type", "STRING"),
                    bigquery.SchemaField("payload", "JSON"),
                ],
            )

            bq_client.load_table_from_uri(
                f"gs://{bucket_raw}/{gcs_path}",
                table_id,
                job_config=job_config,
            ).result()

        # Cursor update: based on max PR updated_at we processed
        if max_pr_updated_at:
            upsert_cursor(
                bq_client,
                project_id,
                dataset,
                "pr_reviews",
                repo,
                datetime.fromisoformat(max_pr_updated_at.replace("Z", "+00:00")),
            )

with DAG(
    dag_id="github_pr_reviews_ingestion",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["github", "ingestion"],
) as dag:
    PythonOperator(
        task_id="extract_upload_load_pr_reviews",
        python_callable=pr_reviews_extract_upload_load,
    )