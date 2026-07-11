# Enterprise Data Reliability & Intelligence Platform

A production-inspired, cloud-native MLOps platform that ingests, validates,
orchestrates, and monitors enterprise data pipelines — built to demonstrate
how large organizations (ING, Google, Microsoft, Amazon, ...) run reliable,
automated data workflows, not to ship another ML model.

## Why this exists

Enterprises ingest millions of records daily from many sources. Missing
records, broken schemas, duplicate rows, failed ETL jobs, and silent data
quality decay usually surface only after they've already corrupted a
dashboard or a model. This platform is designed to catch those problems
**before** they reach downstream consumers, and — via the Root Cause
Analysis engine — explain *why* a pipeline failed instead of just flagging
that it did.

```
CSV / REST API / PostgreSQL
        │
        ▼
  Data Ingestion Layer  (etl/extract.py)
        │
        ▼
  Apache Airflow DAG    (airflow/dags/data_reliability_pipeline.py)
        │
        ▼
  ETL: transform → validate → load   (etl/, validation/)
        │
        ▼
  PostgreSQL warehouse   (scripts/init_db.sql)
        │
        ▼
  Feature Engineering    (feature_engineering/)
        │
        ▼
  MLflow experiment tracking + Isolation Forest anomaly detection (ml/)
        │
        ▼
  Root Cause Analysis engine   (monitoring/rca.py)
        │
        ▼
  Monitoring store + Streamlit dashboard (monitoring/tracker.py, dashboard/)
        │
        ▼
  Alerting  (send_alert task; Slack/email wiring pending)
```

## What makes a run "fail loud, not silent": Root Cause Analysis

Every failure — an extraction error or a failed Data Quality check — is
classified into ranked causes instead of a bare stack trace:

```
Pipeline failed

Reason:
- Missing column(s): value
- Duplicate primary keys (1 rows)
- Data volume anomaly (-98.0%)
```

See `monitoring/rca.py`. It's consumed by the DAG's `on_failure_callback`
and rendered per-run in the Streamlit dashboard.

## Repository layout

```
airflow/dags/       Airflow DAG(s) — thin orchestration only, no business logic
etl/                 extract / transform / load — pure, unit-testable functions
validation/          per-check functions + DataValidationEngine (Data Quality Score)
feature_engineering/ numeric feature matrix builder for the ML layer
ml/                  Isolation Forest anomaly detection + MLflow tracking
monitoring/          pipeline run tracker (Postgres) + Root Cause Analysis engine
dashboard/           Streamlit monitoring UI
docker/              per-service Dockerfiles + Postgres init scripts
scripts/             scripts/init_db.sql — warehouse schema (pipeline_runs, dq_results, ...)
config/              config.yaml + settings.py (env-var aware config loader)
tests/               pytest unit tests for validation, RCA, transform
.github/workflows/   CI: lint, tests, docker builds, compose config validation
```

## Data Quality Score

`validation/engine.py` runs a configurable, weighted set of checks
(required columns, duplicate primary keys, null %, dtype mismatch, IQR
outliers, invalid ranges, volume anomaly vs. baseline) and reduces them to a
single 0–100 score. A run is marked failed below
`validation.dq_score_fail_threshold` (default 70), which triggers the RCA
engine and the failure alert path.

## Running it locally

Requires Docker Desktop running (the daemon, not just the CLI).

```bash
cp .env.example .env      # fill in real secrets before anything beyond local dev
docker compose up --build
```

Services once up:

| Service            | URL                    |
|---------------------|-------------------------|
| Airflow webserver    | http://localhost:8081 (admin/admin) |
| MLflow tracking UI   | http://localhost:5000 |
| Streamlit dashboard  | http://localhost:8501 |
| PostgreSQL           | localhost:5432 |

Drop a CSV into `data/raw/latest.csv`, then trigger the
`data_reliability_pipeline` DAG from the Airflow UI (or `airflow dags trigger`
inside the scheduler container).

**Status note:** this scaffold has been syntax-validated (`docker compose
config`) and unit-tested logic is included, but the full multi-service stack
has not yet been run end-to-end in this environment — Docker Desktop's
engine wasn't running here. Expect to debug on first `docker compose up`
(typical for a stack this size: Airflow DB init timing, first-run MLflow
backend migrations, etc.).

## Running tests

```bash
pip install -r requirements.txt
pytest --cov=. --cov-report=term-missing
ruff check .
```

## Roadmap

- Kafka streaming ingestion
- Kubernetes deployment + Terraform IaC
- Prometheus / Grafana metrics (beyond the Streamlit dashboard)
- Great Expectations / dbt for declarative validation and transforms
- Spark for large-volume ETL
- Vertex AI for managed model serving
- FastAPI service layer + auth
- Slack/email alert channels (hooks already present in `send_alert` /
  `on_pipeline_failure`)
