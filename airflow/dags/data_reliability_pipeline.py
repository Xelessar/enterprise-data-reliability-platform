"""End-to-end data reliability pipeline.

Extract -> Validate -> Transform -> Load -> Run ML -> Generate Report -> Send Alert

Every task is a thin wrapper around a pure function in etl/, validation/, ml/,
or monitoring/ so the actual logic stays unit-testable outside Airflow. Large
objects (DataFrames) never go through XCom — they're written to parquet under
data/processed/ and only the path + small metadata cross task boundaries.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Project root is mounted at /opt/airflow and put on PYTHONPATH (see docker-compose.yml),
# but we guard here too so the DAG also imports cleanly outside the container.
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.settings import get_config, get_db_url  # noqa: E402
from etl.extract import ExtractionError, extract  # noqa: E402
from etl.load import load_to_postgres  # noqa: E402
from etl.transform import transform  # noqa: E402
from ml.anomaly_detection import run_anomaly_detection  # noqa: E402
from monitoring.rca import analyze as rca_analyze  # noqa: E402
from monitoring.tracker import PipelineRunTracker  # noqa: E402
from validation.engine import DataValidationEngine  # noqa: E402

logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _engine():
    from sqlalchemy import create_engine

    return create_engine(get_db_url())


def _tracker() -> PipelineRunTracker:
    return PipelineRunTracker(_engine())


def _parquet_path(run_id: str, stage: str) -> str:
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    safe_run_id = run_id.replace(":", "_").replace("+", "_")
    return os.path.join(PROCESSED_DIR, f"{safe_run_id}__{stage}.parquet")


def extract_data(**context) -> str:
    ti = context["ti"]
    run_id = context["run_id"]
    cfg = get_config()

    tracker = _tracker()
    pipeline_run_id = tracker.start_run(dag_id=context["dag"].dag_id, run_id=run_id, source="csv")
    ti.xcom_push(key="pipeline_run_id", value=pipeline_run_id)

    csv_cfg = cfg["sources"]["csv"]
    csv_dir = csv_cfg["path"] or os.path.join(PROJECT_ROOT, "data/raw")
    try:
        result = extract("csv", path=os.path.join(csv_dir, "latest.csv"))
    except ExtractionError as exc:
        rca = rca_analyze(exception=exc)
        ti.xcom_push(key="rca_summary", value=rca.summary)
        raise

    path = _parquet_path(run_id, "raw")
    result.df.to_parquet(path)
    ti.xcom_push(key="row_count", value=result.row_count)
    return path


def validate_data(**context) -> str:
    ti = context["ti"]
    run_id = context["run_id"]
    cfg = get_config()

    import pandas as pd

    raw_path = ti.xcom_pull(task_ids="extract_data")
    df = pd.read_parquet(raw_path)
    df = transform(df, dtype_map=cfg["validation"].get("expected_dtypes"))

    engine = DataValidationEngine(cfg["validation"])
    report = engine.run(df)

    pipeline_run_id = ti.xcom_pull(key="pipeline_run_id", task_ids="extract_data")
    tracker = _tracker()
    tracker.record_validation(pipeline_run_id, report)

    if not report.passed:
        rca = rca_analyze(validation_report=report)
        ti.xcom_push(key="rca_summary", value=rca.summary)
        raise ValueError(f"Data Quality Score {report.dq_score} below threshold:\n{rca.summary}")

    path = _parquet_path(run_id, "validated")
    df.to_parquet(path)
    return path


def transform_data(**context) -> str:
    """Second-pass, warehouse-shaping transform (validated data is already clean)."""
    ti = context["ti"]
    run_id = context["run_id"]

    import pandas as pd

    validated_path = ti.xcom_pull(task_ids="validate_data")
    df = pd.read_parquet(validated_path)

    path = _parquet_path(run_id, "transformed")
    df.to_parquet(path)
    return path


def load_warehouse(**context) -> None:
    ti = context["ti"]
    cfg = get_config()

    import pandas as pd

    transformed_path = ti.xcom_pull(task_ids="transform_data")
    df = pd.read_parquet(transformed_path)
    df["pipeline_run_id"] = ti.xcom_pull(key="pipeline_run_id", task_ids="extract_data")

    engine = _engine()
    table = cfg["warehouse"]["tables"]["processed"]
    load_to_postgres(df, table_name=table, engine=engine)


def run_ml_model(**context) -> str:
    ti = context["ti"]
    run_id = context["run_id"]
    cfg = get_config()

    import pandas as pd

    transformed_path = ti.xcom_pull(task_ids="transform_data")
    df = pd.read_parquet(transformed_path)

    result = run_anomaly_detection(
        df, ml_config=cfg["ml"], tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    )

    pipeline_run_id = ti.xcom_pull(key="pipeline_run_id", task_ids="extract_data")
    tracker = _tracker()
    tracker.record_anomalies(pipeline_run_id, result.anomaly_count)

    path = _parquet_path(run_id, "scored")
    result.scored_df.to_parquet(path)
    ti.xcom_push(key="anomaly_count", value=result.anomaly_count)
    ti.xcom_push(key="mlflow_run_id", value=result.mlflow_run_id)
    return path


def generate_report(**context) -> None:
    ti = context["ti"]
    pipeline_run_id = ti.xcom_pull(key="pipeline_run_id", task_ids="extract_data")
    tracker = _tracker()
    tracker.complete_run(pipeline_run_id, status="success")
    logger.info("Pipeline run %s completed successfully", pipeline_run_id)


def send_alert(**context) -> None:
    """Success-path notification. Failures are handled by on_pipeline_failure below."""
    ti = context["ti"]
    anomaly_count = ti.xcom_pull(key="anomaly_count", task_ids="run_ml_model")
    logger.info("ALERT[info]: pipeline succeeded, %s anomalies detected", anomaly_count)
    # Wire a real channel here (Slack webhook / SMTP) once ALERT_EMAIL_TO / SLACK_WEBHOOK_URL are set.


def on_pipeline_failure(context) -> None:
    ti = context["ti"]
    exception = context.get("exception")

    pipeline_run_id = ti.xcom_pull(key="pipeline_run_id", task_ids="extract_data")
    rca_summary = ti.xcom_pull(key="rca_summary")

    if rca_summary is None:
        rca = rca_analyze(exception=exception)
        rca_summary = rca.summary

    logger.error("ALERT[failure]: %s", rca_summary)

    if pipeline_run_id is not None:
        tracker = _tracker()
        tracker.complete_run(pipeline_run_id, status="failed", rca_summary=rca_summary)


with DAG(
    dag_id="data_reliability_pipeline",
    description="Extract -> Validate -> Transform -> Load -> Run ML -> Generate Report -> Send Alert",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_failure_callback=on_pipeline_failure,
    tags=["data-reliability", "mlops"],
) as dag:
    t_extract = PythonOperator(task_id="extract_data", python_callable=extract_data)
    t_validate = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t_transform = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t_load = PythonOperator(task_id="load_warehouse", python_callable=load_warehouse)
    t_ml = PythonOperator(task_id="run_ml_model", python_callable=run_ml_model)
    t_report = PythonOperator(task_id="generate_report", python_callable=generate_report)
    t_alert = PythonOperator(
        task_id="send_alert", python_callable=send_alert, trigger_rule=TriggerRule.ALL_SUCCESS
    )

    t_extract >> t_validate >> t_transform >> t_load >> t_ml >> t_report >> t_alert
