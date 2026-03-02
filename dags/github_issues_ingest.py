from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timezone
import os, json, uuid

from google.cloud import storage, bigquery

from ingestion.github.cursor import get_cursor, upsert_cursor
from ingestion.github.extract_issues import fetch_issues

def issues_extract_upload_load():
    token = Variable.get("GITHUB_TOKEN")
    repos = json.loads(Variable.get("GITHUB_REPOS"))

    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_raw = os.environ["GCS_BUCKET_RAW"]
    dataset = os.environ.get("BQ_DATASET_RAW", "gh_raw")
    table_id = f"{project_id}.{dataset}.github_issues_raw"

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = str(uuid.uuid4())
    os.makedirs("/opt/airflow/logs/tmp", exist_ok=True)

    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_raw)

    for repo in repos:
        since_ts = get_cursor(bq_client, project_id, dataset, "issues", repo, fallback_days=7)
        issues = fetch_issues(token, repo, since_ts)

        # Determine new watermark (max updated_at from returned issues)
        max_updated_at = None
        for it in issues:
            u = it.get("updated_at")
            if u and (max_updated_at is None or u > max_updated_at):
                max_updated_at = u

        # Chunk output to avoid timeouts
        chunk_size = 2000
        chunks = [issues[i:i+chunk_size] for i in range(0, len(issues), chunk_size)]

        for idx, chunk in enumerate(chunks):
            local_path = f"/opt/airflow/logs/tmp/{repo.replace('/', '_')}_{ingestion_date}_{run_id}_issues_part{idx}.jsonl"
            with open(local_path, "w", encoding="utf-8") as f:
                for issue in chunk:
                    row = {
                        "ingestion_date": ingestion_date,
                        "repo_full_name": repo,
                        "event_type": "issue",
                        "payload": issue,
                    }
                    f.write(json.dumps(row)); f.write("\n")

            gcs_path = f"github/issues/dt={ingestion_date}/repo={repo.replace('/', '__')}/run_id={run_id}/part={idx}/data.jsonl"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path, timeout=300, num_retries=5)

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
            bq_client.load_table_from_uri(
                f"gs://{bucket_raw}/{gcs_path}",
                table_id,
                job_config=job_config,
            ).result()

        # Update cursor only after successful load(s)
        if max_updated_at:
            upsert_cursor(
                bq_client,
                project_id,
                dataset,
                "issues",
                repo,
                datetime.fromisoformat(max_updated_at.replace("Z", "+00:00")),
            )

with DAG(
    dag_id="github_issues_ingestion",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["github", "ingestion"],
) as dag:
    PythonOperator(
        task_id="extract_upload_load_issues",
        python_callable=issues_extract_upload_load,
    )