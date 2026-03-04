from datetime import datetime, timezone
import os

from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)


with DAG(
    dag_id="github_dbt_snapshot_manual",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["github", "dbt", "snapshot"],
) as dag:
    dbt_deps_snapshot = BashOperator(
        task_id="dbt_deps_snapshot",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_deps_snapshot >> dbt_snapshot
