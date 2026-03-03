import json
import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery


PROJECT_ENV = "GCP_PROJECT_ID"
RAW_DATASET_ENV = "BQ_DATASET_RAW"
RAW_DATASET_DEFAULT = "gh_raw"

INGESTION_ENTITIES = ["pulls", "issues", "pr_reviews"]


def _load_repo_filter():
    raw_repos = Variable.get("GITHUB_REPOS", default_var="[]")
    try:
        parsed = json.loads(raw_repos)
    except json.JSONDecodeError:
        return []

    if not isinstance(parsed, list):
        return []

    return [repo for repo in parsed if isinstance(repo, str) and repo.strip()]


def check_new_data_since_start(**context):
    project_id = os.environ.get(PROJECT_ENV)
    if not project_id:
        raise ValueError("Missing required environment variable: GCP_PROJECT_ID")

    dataset = os.environ.get(RAW_DATASET_ENV, RAW_DATASET_DEFAULT)
    table_ref = f"`{project_id}.{dataset}.ingestion_cursors`"

    dag_run = context.get("dag_run")
    run_start = dag_run.start_date if dag_run else None
    if run_start is None:
        run_start = datetime.now(timezone.utc)
    else:
        run_start = run_start.astimezone(timezone.utc)

    repo_filter = _load_repo_filter()

    repo_clause = ""
    query_parameters = [
        bigquery.ArrayQueryParameter("entities", "STRING", INGESTION_ENTITIES),
        bigquery.ScalarQueryParameter("run_start", "TIMESTAMP", run_start),
    ]

    if repo_filter:
        repo_clause = "and repo_full_name in unnest(@repos)"
        query_parameters.append(
            bigquery.ArrayQueryParameter("repos", "STRING", repo_filter)
        )

    query = f"""
        select count(*) as updated_cursor_count
        from {table_ref}
        where entity in unnest(@entities)
          and updated_at >= @run_start
          {repo_clause}
    """

    client = bigquery.Client(project=project_id)
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
    )
    rows = list(job.result())
    updated_cursor_count = int(rows[0]["updated_cursor_count"]) if rows else 0

    print(
        "cursor update check: "
        f"project={project_id}, dataset={dataset}, run_start_utc={run_start.isoformat()}, "
        f"repo_filter_count={len(repo_filter)}, updated_cursor_count={updated_cursor_count}"
    )
    return updated_cursor_count


def branch_on_new_data(**context):
    task_instance = context["ti"]
    updated_cursor_count = int(
        task_instance.xcom_pull(task_ids="check_new_data_since_start") or 0
    )
    print(f"branching on updated_cursor_count={updated_cursor_count}")

    if updated_cursor_count > 0:
        return "trigger_dbt_transform"
    return "skip_dbt_no_new_data"


with DAG(
    dag_id="github_ingest_transform_master",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["github", "orchestration", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    trigger_pulls_ingestion = TriggerDagRunOperator(
        task_id="trigger_pulls_ingestion",
        trigger_dag_id="github_pulls_ingestion",
        trigger_run_id="master_pulls__{{ ts_nodash }}",
        execution_date="{{ dag_run.logical_date.isoformat() }}",
        conf={
            "triggered_by": "github_ingest_transform_master",
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ run_id }}",
            "parent_logical_date": "{{ dag_run.logical_date.isoformat() }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
    )

    trigger_issues_ingestion = TriggerDagRunOperator(
        task_id="trigger_issues_ingestion",
        trigger_dag_id="github_issues_ingestion",
        trigger_run_id="master_issues__{{ ts_nodash }}",
        execution_date="{{ dag_run.logical_date.isoformat() }}",
        conf={
            "triggered_by": "github_ingest_transform_master",
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ run_id }}",
            "parent_logical_date": "{{ dag_run.logical_date.isoformat() }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
    )

    trigger_pr_reviews_ingestion = TriggerDagRunOperator(
        task_id="trigger_pr_reviews_ingestion",
        trigger_dag_id="github_pr_reviews_ingestion",
        trigger_run_id="master_pr_reviews__{{ ts_nodash }}",
        execution_date="{{ dag_run.logical_date.isoformat() }}",
        conf={
            "triggered_by": "github_ingest_transform_master",
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ run_id }}",
            "parent_logical_date": "{{ dag_run.logical_date.isoformat() }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
    )

    wait_for_pulls_ingestion = ExternalTaskSensor(
        task_id="wait_for_pulls_ingestion",
        external_dag_id="github_pulls_ingestion",
        external_task_id="extract_upload_load_pulls",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 4,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    wait_for_issues_ingestion = ExternalTaskSensor(
        task_id="wait_for_issues_ingestion",
        external_dag_id="github_issues_ingestion",
        external_task_id="extract_upload_load_issues",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 4,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    wait_for_pr_reviews_ingestion = ExternalTaskSensor(
        task_id="wait_for_pr_reviews_ingestion",
        external_dag_id="github_pr_reviews_ingestion",
        external_task_id="extract_upload_load_pr_reviews",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 4,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    check_new_data = PythonOperator(
        task_id="check_new_data_since_start",
        python_callable=check_new_data_since_start,
    )

    decide_dbt_branch = BranchPythonOperator(
        task_id="decide_dbt_branch",
        python_callable=branch_on_new_data,
    )

    trigger_dbt_transform = TriggerDagRunOperator(
        task_id="trigger_dbt_transform",
        trigger_dag_id="github_dbt_transform_manual",
        trigger_run_id="master_dbt__{{ ts_nodash }}",
        execution_date="{{ dag_run.logical_date.isoformat() }}",
        conf={
            "triggered_by": "github_ingest_transform_master",
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ run_id }}",
            "parent_logical_date": "{{ dag_run.logical_date.isoformat() }}",
            "updated_cursor_count": "{{ ti.xcom_pull(task_ids='check_new_data_since_start') }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
    )

    wait_for_dbt_test = ExternalTaskSensor(
        task_id="wait_for_dbt_test",
        external_dag_id="github_dbt_transform_manual",
        external_task_id="dbt_test",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 4,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    skip_dbt_no_new_data = EmptyOperator(task_id="skip_dbt_no_new_data")

    orchestration_complete = EmptyOperator(
        task_id="orchestration_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> [
        trigger_pulls_ingestion,
        trigger_issues_ingestion,
        trigger_pr_reviews_ingestion,
    ]

    trigger_pulls_ingestion >> wait_for_pulls_ingestion
    trigger_issues_ingestion >> wait_for_issues_ingestion
    trigger_pr_reviews_ingestion >> wait_for_pr_reviews_ingestion

    [
        wait_for_pulls_ingestion,
        wait_for_issues_ingestion,
        wait_for_pr_reviews_ingestion,
    ] >> check_new_data >> decide_dbt_branch

    decide_dbt_branch >> trigger_dbt_transform >> wait_for_dbt_test >> orchestration_complete
    decide_dbt_branch >> skip_dbt_no_new_data >> orchestration_complete
