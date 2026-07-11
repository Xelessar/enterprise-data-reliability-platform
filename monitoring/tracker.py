"""Pipeline run tracking: the source of truth behind the monitoring dashboard.

One row per DAG run in `pipeline_runs`; per-check detail in `dq_results`.
Kept as a thin SQLAlchemy wrapper (no ORM) so it's easy to read and to swap
Postgres for BigQuery later.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from validation.engine import ValidationReport


class PipelineRunTracker:
    def __init__(self, engine: Engine):
        self.engine = engine

    def start_run(self, dag_id: str, run_id: str, source: str | None = None) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO pipeline_runs (dag_id, run_id, source, status, started_at)
                    VALUES (:dag_id, :run_id, :source, 'running', :started_at)
                    ON CONFLICT (dag_id, run_id) DO UPDATE SET status = 'running'
                    RETURNING id
                    """
                ),
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "source": source,
                    "started_at": datetime.now(timezone.utc),
                },
            )
            return result.scalar_one()

    def record_validation(self, pipeline_run_id: int, report: ValidationReport) -> None:
        with self.engine.begin() as conn:
            for check in report.checks:
                conn.execute(
                    text(
                        """
                        INSERT INTO dq_results (pipeline_run_id, check_name, passed, weight, details)
                        VALUES (:run_id, :name, :passed, :weight, :details)
                        """
                    ),
                    {
                        "run_id": pipeline_run_id,
                        "name": check.name,
                        "passed": check.passed,
                        "weight": check.weight,
                        "details": json.dumps(check.details),
                    },
                )
            conn.execute(
                text("UPDATE pipeline_runs SET dq_score = :score, row_count = :rows WHERE id = :id"),
                {"score": report.dq_score, "rows": report.row_count, "id": pipeline_run_id},
            )

    def record_anomalies(
        self, pipeline_run_id: int, scored_df: pd.DataFrame, mlflow_run_id: str | None = None
    ) -> int:
        """Persist per-record anomaly detail (not just a count) so the dashboard
        can show *which* records need review, not just how many."""
        flagged = scored_df[scored_df["is_anomaly"]]
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE pipeline_runs SET anomaly_count = :count WHERE id = :id"),
                {"count": len(flagged), "id": pipeline_run_id},
            )
            for _, row in flagged.iterrows():
                conn.execute(
                    text(
                        """
                        INSERT INTO ml_anomalies
                            (pipeline_run_id, record_id, anomaly_score, is_anomaly,
                             reason, severity, mlflow_run_id)
                        VALUES (:pipeline_run_id, :record_id, :anomaly_score, TRUE,
                                :reason, :severity, :mlflow_run_id)
                        """
                    ),
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "record_id": str(row["id"]),
                        "anomaly_score": float(row["anomaly_score"]),
                        "reason": row.get("anomaly_reason"),
                        "severity": row.get("anomaly_severity"),
                        "mlflow_run_id": mlflow_run_id,
                    },
                )
        return len(flagged)

    def anomalies_for_run(self, pipeline_run_id: int) -> list[dict[str, Any]]:
        """Flagged records for a run, enriched with their actual value/timestamp
        by joining back to processed_records on the shared source id."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT a.record_id, a.anomaly_score, a.reason, a.severity,
                           a.mlflow_run_id, a.detected_at,
                           p."timestamp" AS record_timestamp, p.value AS record_value
                    FROM ml_anomalies a
                    LEFT JOIN processed_records p
                        ON p.pipeline_run_id = a.pipeline_run_id
                       AND p.id::text = a.record_id
                    WHERE a.pipeline_run_id = :id
                    ORDER BY a.anomaly_score ASC
                    """
                ),
                {"id": pipeline_run_id},
            ).mappings().all()
            return [dict(r) for r in rows]

    def complete_run(
        self, pipeline_run_id: int, status: str, rca_summary: str | None = None
    ) -> None:
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT started_at FROM pipeline_runs WHERE id = :id"), {"id": pipeline_run_id}
            ).mappings().one()
            finished_at = datetime.now(timezone.utc)
            duration = (finished_at - row["started_at"]).total_seconds()
            conn.execute(
                text(
                    """
                    UPDATE pipeline_runs
                    SET status = :status, finished_at = :finished_at,
                        duration_seconds = :duration, rca_summary = :rca_summary
                    WHERE id = :id
                    """
                ),
                {
                    "status": status,
                    "finished_at": finished_at,
                    "duration": duration,
                    "rca_summary": rca_summary,
                    "id": pipeline_run_id,
                },
            )

    def recent_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, dag_id, run_id, source, status, started_at, finished_at,
                           duration_seconds, row_count, dq_score, anomaly_count, rca_summary
                    FROM pipeline_runs ORDER BY started_at DESC LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).mappings().all()
            return [dict(r) for r in rows]

    def success_rate(self, window: int = 100) -> float:
        runs = self.recent_runs(limit=window)
        finished = [r for r in runs if r["status"] in ("success", "failed")]
        if not finished:
            return 0.0
        successes = sum(1 for r in finished if r["status"] == "success")
        return round(successes / len(finished) * 100, 2)
