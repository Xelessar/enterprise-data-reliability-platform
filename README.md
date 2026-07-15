# Enterprise Data Reliability & Intelligence Platform

A production-inspired, cloud-native MLOps platform that ingests, validates,
orchestrates, and monitors enterprise data pipelines — built to demonstrate
how large organizations ( Google, Microsoft, Amazon, ...) run reliable,
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
  Slack alerting on success/failure  (monitoring/alerts.py)
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

## What's implemented

- **ETL**: CSV / REST API / PostgreSQL extractors (`etl/extract.py`), reusable transform pipeline, Postgres loader, optional BigQuery loader wired into the DAG (`etl/load.py::load_to_bigquery`, best-effort secondary sink — see below)
- **Data Quality Score**: 7 configurable checks reduced to a single 0–100 score (see below)
- **Root Cause Analysis**: ranked, human-readable failure causes instead of stack traces
- **Anomaly detection**: Isolation Forest, MLflow-tracked (params/metrics/model artifacts), with a per-record human-readable reason and severity tier — not just a bare score
- **Airflow orchestration**: extract → validate → transform → load → run_ml_model → generate_report → send_alert, with retries and a DAG-level failure callback
- **Monitoring dashboard** (Streamlit): DQ score trend, run history, flagged-record drill-down with severity/reason/recommended action, CSV export
- **Slack alerting**: real Incoming Webhook notifications on both success and failure, linking back to the dashboard
- **CI**: lint (ruff), unit tests (pytest), Docker image builds (GitHub Actions)

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

**Status note:** verified end-to-end against a real local run — the DAG
completes all 7 tasks successfully, MLflow logs a real experiment run with a
model artifact, anomalies are flagged with reasons in the dashboard, and
Slack receives a real alert. Getting there required fixing several genuine
dependency conflicts (pandas/SQLAlchemy/Airflow version pinning, an MLflow
artifact-store permission mismatch) — see the commit history for details if
you hit similar issues wiring Airflow + MLflow + pandas 2.x together.

## BigQuery (optional secondary warehouse)

`load_warehouse` always writes to Postgres first; if `GCP_PROJECT_ID` and
`BIGQUERY_DATASET` are set, it then also loads the same batch into BigQuery
(`etl/load.py::load_to_bigquery`), auto-creating the dataset on first run. A
BigQuery failure is logged and swallowed rather than failing the DAG —
Postgres is the source of truth for every downstream task, so a transient
GCP auth/quota error shouldn't block the daily run.

To enable it:

1. Create a GCP project and enable the BigQuery API.
2. Create a service account with **BigQuery Data Editor** + **BigQuery Job
   User** roles, download its JSON key, and save it as `./secrets/gcp-key.json`
   (gitignored — never commit this file).
3. In `.env`, set `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, and
   `GOOGLE_APPLICATION_CREDENTIALS=1` (any non-empty value — see
   `docker-compose.yml`'s `airflow-env` anchor for how the actual in-container
   path is derived).
4. `docker compose up --build airflow-scheduler airflow-webserver` and
   trigger the DAG — the `load_warehouse` task logs `Loaded N rows into
   BigQuery table <project>.<dataset>.<table>` on success.

GCP client libraries are installed only in the Airflow image
(`requirements-gcp.txt`, layered separately in `docker/airflow.Dockerfile`) —
mixing `google-cloud-bigquery`'s dependency chain into the shared local
`.venv` used by mlflow/streamlit forces `protobuf` to a version neither of
them supports. `tests/test_load.py` covers `load_to_bigquery`'s logic
(dataset auto-create, table-id construction) against a stubbed
`google.cloud.bigquery` module instead of the real SDK, for the same reason.

## Running tests

```bash
pip install -r requirements.txt
pytest --cov=. --cov-report=term-missing
ruff check .
```

## Roadmap

In priority order — each one builds on what's already working rather than
starting a parallel track.

1. **Cloud Storage as a raw-data landing zone** — currently only BigQuery is
   wired up (see above); GCS would replace the local `data/raw/` CSV drop for
   a fully cloud-native ingestion path.
2. **PDF report generation** — `generate_report` currently only marks the
   run complete; it should produce an actual report (DQ score, flagged
   records, RCA) for stakeholders who won't open the dashboard.
3. **Looker Studio dashboard** — once data lives in BigQuery, connect Looker
   Studio for a shareable, no-code view alongside the Streamlit one.
4. **Vertex AI** — managed training/serving for the anomaly detection model
   instead of local scikit-learn + MLflow.
5. **Kubernetes deployment** — move off `docker compose` for anything beyond
   local dev.
6. Further out: Kafka streaming ingestion, Spark for large-volume ETL, Great
   Expectations/dbt for declarative validation, email alert channel,
   FastAPI service layer + auth.
