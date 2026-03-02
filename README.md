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

## 🛠️ Tech Stack Focus
- **Languages:** Python, SQL
- **Orchestration:** Apache Airflow
- **Data Transformation:** dbt (Data Build Tool)
- **Data Warehouse:** Google BigQuery
- **Cloud Storage:** Google Cloud Storage (GCS)
- **Containerization:** Docker

## 🤖 AI Usage in Development
Artificial Intelligence (LLMs) was strategically used during the development of this pipeline to accelerate the workflow:
- **Migration & Refactoring:** Assisted in migrating dbt project configurations and SQL dialects from another data warehouse architecture into native BigQuery Standard SQL, while maintaining the intended logic.
- **Debugging Orchestration:** Served as a debugging partner to rapidly parse Airflow stack traces when encountering complex pipeline failures, minimizing downtime.
- **Repository Security:** Provided precise git strategies for security remediation (e.g., rewriting branch history) to securely scrub accidental secrets from version control.
