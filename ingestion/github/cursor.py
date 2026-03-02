from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
from google.cloud import bigquery

def default_cutoff(days: int = 7) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)

def get_cursor(
    bq: bigquery.Client,
    project_id: str,
    dataset: str,
    entity: str,
    repo_full_name: str,
    fallback_days: int = 7
) -> datetime:
    """
    Returns watermark timestamp. If missing, returns now - fallback_days.
    """
    table = f"{project_id}.{dataset}.ingestion_cursors"
    sql = f"""
      SELECT last_updated_at
      FROM `{table}`
      WHERE entity = @entity AND repo_full_name = @repo
      LIMIT 1
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("entity", "STRING", entity),
                bigquery.ScalarQueryParameter("repo", "STRING", repo_full_name),
            ]
        ),
    )
    rows = list(job.result())
    if not rows or rows[0]["last_updated_at"] is None:
        return default_cutoff(fallback_days)
    return rows[0]["last_updated_at"]

def upsert_cursor(
    bq: bigquery.Client,
    project_id: str,
    dataset: str,
    entity: str,
    repo_full_name: str,
    last_updated_at: datetime,
) -> None:
    """
    MERGE cursor row after successful ingestion.
    """
    table = f"{project_id}.{dataset}.ingestion_cursors"
    now = datetime.now(timezone.utc)

    sql = f"""
    MERGE `{table}` T
    USING (
      SELECT
        @entity AS entity,
        @repo AS repo_full_name,
        @last_updated_at AS last_updated_at,
        @now AS last_success_run,
        @now AS updated_at
    ) S
    ON T.entity = S.entity AND T.repo_full_name = S.repo_full_name
    WHEN MATCHED THEN UPDATE SET
      last_updated_at = S.last_updated_at,
      last_success_run = S.last_success_run,
      updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT(entity, repo_full_name, last_updated_at, last_success_run, updated_at)
    VALUES(S.entity, S.repo_full_name, S.last_updated_at, S.last_success_run, S.updated_at)
    """

    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("entity", "STRING", entity),
                bigquery.ScalarQueryParameter("repo", "STRING", repo_full_name),
                bigquery.ScalarQueryParameter("last_updated_at", "TIMESTAMP", last_updated_at),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            ]
        ),
    ).result()