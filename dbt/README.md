# github_health_analytics dbt Project

This dbt project handles the transformation layer of the GitHub Health Analytics pipeline, moving raw JSON data from BigQuery into staging, intermediate, and marts layers. It was adapted from the `Snowflake-ELT-Pipeline-with-dbt-and-Airflow-main` repository to use the `dbt-bigquery` adapter.

## Setup Instructions

### Local Execution

1. **Install Dependencies**:
   Ensure you have `dbt-bigquery` installed in your environment.
   ```bash
   pip install dbt-bigquery
   ```

2. **Set Environment Variables**:
   This project delegates configuration to environment variables via `profiles.yml`.
   ```bash
   export DBT_BQ_PROJECT="your-gcp-project-id"
   export DBT_BQ_DATASET="gh_analytics"
   export DBT_BQ_KEYFILE="/path/to/your/service-account-key.json"
   ```

3. **Verify Connection**:
   From the `/dbt` directory, verify dbt can connect to BigQuery.
   ```bash
   dbt debug
   ```

4. **Run Models**:
   ```bash
   dbt deps
   dbt run
   dbt test
   ```

### Airflow Execution

To orchestrate these dbt models via Airflow, you can use the `BashOperator` or a package like `astronomer-cosmos`.

**Using BashOperator**:
Configure your DAG to pass the same environment variables locally.
```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt && dbt run',
    env={
        'DBT_BQ_PROJECT': '{{ var.value.gcp_project_id }}',
        'DBT_BQ_DATASET': 'gh_analytics',
        'DBT_BQ_KEYFILE': '/path/to/key.json',
        **os.environ
    }
)
```
