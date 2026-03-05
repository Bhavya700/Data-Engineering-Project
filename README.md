# GitHub Health Analytics

![Architecture](https://img.shields.io/badge/Architecture-ELT-blue)
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.x-red)
![dbt](https://img.shields.io/badge/dbt-Core-orange)
![GCP](https://img.shields.io/badge/GCP-BigQuery%20%7C%20GCS-green)

GitHub Health Analytics is an automated data pipeline designed to measure and monitor how efficiently software development teams are working. It extracts activity data directly from GitHub (Pull Requests, Issues, Reviews), loads it into a secure cloud data warehouse, and transforms it into actionable, daily health metrics for each monitored repository.

## 🎯 Business Value

Measuring engineering velocity and repository health allows teams to:
- **Track Code Velocity:** Monitor average cycle times, such as time from pull request creation to the first review, and time to merge.
- **Identify Bottlenecks:** Easily identify "stale" issues that have been forgotten or are blocking development.
- **Ensure Code Quality:** Track the size of PRs and review activity to maintain high code standards.

## 🏗️ Architecture (ELT Pipeline)

The project follows a modern ELT (Extract, Load, Transform) pattern utilizing powerful, industry-standard tools:

1. **Extract & Load (Python & Google Cloud Storage -> BigQuery)**:
   - Python code incrementally fetches data from the GitHub API using a "cursor" based on timestamps (`updated_at`). This ensures only new or updated PRs, Issues, and Reviews are fetched.
   - The raw JSON data is uploaded to **Google Cloud Storage (GCS)** for a durable data lake backup.
   - The data is then directly appended into raw tables in **Google BigQuery**.

2. **Orchestration (Apache Airflow)**:
   - The extraction process is orchestrated by **Apache Airflow** running locally via Docker.
   - Individual DAGs (Directed Acyclic Graphs) handle the scheduling, dependency management, retries, and failure tracking of the ingestion scripts on a daily basis.

3. **Transformation (dbt - data build tool)**:
   - Once the raw JSON data lands in BigQuery, **dbt** is used to clean, model, and aggregate the data.
   - *Staging Layer*: Extracts specific relevant fields (like author, lines changed, timestamps) from the complex JSON and casts them into strict data types while deduplicating records.
   - *Marts Layer*: Aggregates the cleaned data into business-ready metrics, like the `mart_repo_health_daily` model, which joins PR and Issue data to produce snapshot metrics per repository per day.

## 📂 Project Structure

```bash
github-health-analytics/
├── .env                  # Environment Variables (Local)
├── airflow_env/          # Docker-compose & Dockerfile for running Airflow locally
├── dags/                 # Airflow DAGs that trigger ingestion
│   └── github_pulls_ingest.py, etc.
├── ingestion/            # Python extraction logic (API requests, Chunking, BQ loading)
│   └── github/           # GitHub-specific ingestion scripts
└── dbt/                  # dbt project definition with BigQuery configuration
    └── models/
        ├── staging/      # SQL models normalizing raw JSON from BigQuery
        └── marts/        # Aggregated business logic SQL models
```

## 🚀 Setup & Execution

### Prerequisites
- Docker and Docker Compose
- Google Cloud Project (BigQuery & GCS enabled)
- Google Cloud Service Account with necessary permissions
- A GitHub Personal Access Token (PAT)
- dbt Core installed locally

### Instructions

1. **Clone the repository**:
   ```bash
   git clone <repo_url>
   cd github-health-analytics
   ```

2. **Configure Environment Variables**:
   Create a `.env` file in the root based on your GCP and GitHub credentials. Place your Google Cloud Service Account JSON key in `airflow_env/gcp-key.json`.

3. **Start Airflow**:
   ```bash
   cd airflow_env
   docker-compose up -d
   ```

4. **Trigger Ingestion**:
   Navigate to the Airflow UI (typically `http://localhost:8080`) and trigger the DAGs manually or let them run on their designated schedules.

5. **Run dbt Transformations**:
   ```bash
   cd dbt
   dbt run
   ```

## ✅ CI + Docs Publish 

This repo now includes GitHub Actions workflows for dbt validation and docs publication:

- `.github/workflows/dbt_ci.yml`
- `.github/workflows/dbt_docs_publish.yml`

### Required GitHub configuration

Set these in your repository settings before running workflows.

1. **Secret**
   - `GCP_SA_KEY_JSON`: full service account JSON content

2. **Repository variables**
   - `GCP_PROJECT_ID=winged-journey-488904-q9`
   - `DBT_BQ_DATASET=gh_analytics`
   - `DBT_BQ_LOCATION=us-west1`
   - `GCS_BUCKET_ARTIFACTS=gh-artifacts-de`

### Workflow behavior

1. **dbt CI** (`dbt_ci.yml`)
   - Trigger: pull requests + pushes to `main`
   - Runs:
     - `dbt deps`
     - `dbt parse`
     - `dbt compile`
     - `dbt test --select staging`
   - Fails fast when required secrets/vars are missing.

2. **dbt docs publish** (`dbt_docs_publish.yml`)
   - Trigger: manual (`workflow_dispatch`)
   - Runs:
     - `dbt deps`
     - `dbt docs generate`
   - Publishes to:
     - `gs://gh-artifacts-de/dbt-docs/latest/`
     - `gs://gh-artifacts-de/dbt-docs/runs/<github_run_id>/`
   - Uploads `dbt/target` as a GitHub artifact.

### Verification checklist

1. Open a PR and confirm `dbt-ci` passes.
2. Manually trigger `dbt-docs-publish`.
3. Verify docs files exist in GCS (`index.html`, `manifest.json`, `catalog.json`).
4. Re-run Airflow DAG `github_dbt_transform_manual` and confirm `dbt_deps -> dbt_run -> dbt_test` still succeeds.

## Orchestration Master DAG

The project now includes a master orchestration DAG:

- `github_ingest_transform_master`

### Behavior

1. Triggers these ingestion DAGs in parallel:
   - `github_pulls_ingestion`
   - `github_issues_ingestion`
   - `github_pr_reviews_ingestion`
2. Waits for their terminal tasks via `ExternalTaskSensor(mode="reschedule")`.
3. Checks `gh_raw.ingestion_cursors.updated_at` for updates since master run start.
4. If updates exist, triggers `github_dbt_transform_manual` and waits for `dbt_test`.
5. If no updates exist, skips dbt and finishes successfully.

### Run Flow

1. Open Airflow UI.
2. Trigger `github_ingest_transform_master` manually.
3. Verify branch outcome:
   - `trigger_dbt_transform` path when new data exists.
   - `skip_dbt_no_new_data` path when no new data exists.

`github_dbt_transform_manual` remains available for manual fallback runs.


### New snapshot DAG

- `github_dbt_snapshot_manual`

It runs:
1. `dbt_deps_snapshot`
2. `dbt_snapshot`

This DAG is intentionally separate from `github_ingest_transform_master` so snapshots remain optional and do not slow every orchestration run.

### Manual refresh run order

1. Trigger `github_ingest_transform_master` (ingestion + transform path).
2. Trigger `github_dbt_snapshot_manual` (history capture path).
3. Refresh dashboard.

### Looker Studio dashboard mapping

Build the dashboard in Looker Studio with BigQuery data sources:

1. Primary table:
   - `gh_analytics.mart_repo_health_daily`
2. Supporting tables:
   - `gh_analytics.fact_pr_cycle_time`
   - `gh_analytics.fact_issue_aging`

Suggested first visuals:
1. Time series: `prs_created_count`, `prs_merged_count`
2. Time series: `open_issues_count`, `stale_issues_count`
3. Scorecards:
   - average `avg_time_to_first_review_hours`
   - average `avg_time_to_merge_hours`
4. Table: repos ordered by `stale_issues_count`

Suggested controls:
1. Date range control
2. `repo_full_name` filter control

After publishing the report, update:
- `dbt/models/exposures.yml` `repo_health_dashboard.url`

## 🛠️ Tech Stack Focus
- **Languages:** Python, SQL
- **Orchestration:** Apache Airflow
- **Data Transformation:** dbt (Data Build Tool)
- **Data Warehouse:** Google BigQuery
- **Cloud Storage:** Google Cloud Storage (GCS)
- **Containerization:** Docker
