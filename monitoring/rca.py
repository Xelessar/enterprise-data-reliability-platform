"""Root Cause Analysis engine.

Turns raw failure signals (an ExtractionError, a failed ValidationReport, or
both) into a ranked, human-readable list of causes — the thing that separates
"pipeline failed" from "pipeline failed because column `customer_id` is
missing and API latency spiked". Consumed by the Airflow DAG's failure
callback and rendered in the Streamlit dashboard.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from etl.extract import ExtractionError
from validation.engine import ValidationReport

# reason_code (from ExtractionError) -> human message template
_EXTRACTION_REASON_MESSAGES = {
    "missing_source_file": "Source file not found ({path})",
    "empty_source": "Source file is empty ({path})",
    "api_timeout": "API timeout ({url})",
    "api_error_status": "API returned HTTP {status_code} ({url})",
    "db_query_failed": "Database query failed",
}

_SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


@dataclass
class RootCause:
    category: str  # e.g. "ingestion", "schema", "data_quality", "volume"
    message: str
    severity: str  # critical | high | medium | low
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class RCAReport:
    causes: list[RootCause]
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def summary(self) -> str:
        if not self.causes:
            return "Pipeline failed\n\nReason: unknown (no diagnostic signal captured)"
        ordered = sorted(self.causes, key=lambda c: _SEVERITY_ORDER.get(c.severity, 9))
        lines = ["Pipeline failed", "", "Reason:"]
        lines += [f"- {c.message}" for c in ordered]
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "evaluated_at": self.evaluated_at.isoformat(),
            "summary": self.summary,
            "causes": [
                {"category": c.category, "message": c.message, "severity": c.severity, "evidence": c.evidence}
                for c in self.causes
            ],
        }


def classify_extraction_error(exc: ExtractionError) -> RootCause:
    template = _EXTRACTION_REASON_MESSAGES.get(exc.reason_code, str(exc))
    try:
        message = template.format(**exc.context)
    except (KeyError, IndexError):
        message = template
    severity = "critical" if exc.reason_code in {"missing_source_file", "db_query_failed"} else "high"
    return RootCause(category="ingestion", message=message, severity=severity, evidence=exc.context)


def classify_validation_report(report: ValidationReport) -> list[RootCause]:
    causes: list[RootCause] = []
    for check in report.failing_checks():
        d = check.details
        if check.name == "required_columns":
            causes.append(
                RootCause(
                    category="schema",
                    message=f"Missing column(s): {', '.join(d['missing_columns'])}",
                    severity="critical",
                    evidence=d,
                )
            )
        elif check.name == "dtypes":
            cols = ", ".join(d["mismatches"].keys())
            causes.append(
                RootCause(
                    category="schema",
                    message=f"Schema changed: dtype mismatch in {cols}",
                    severity="high",
                    evidence=d,
                )
            )
        elif check.name == "duplicates":
            causes.append(
                RootCause(
                    category="data_quality",
                    message=f"Duplicate primary keys ({d['duplicate_count']} rows)",
                    severity="high",
                    evidence=d,
                )
            )
        elif check.name == "null_percentage":
            cols = ", ".join(d["failing_columns"].keys())
            causes.append(
                RootCause(
                    category="data_quality",
                    message=f"High null percentage in column(s): {cols}",
                    severity="medium",
                    evidence=d,
                )
            )
        elif check.name == "outliers":
            pct = round(d["outlier_ratio"] * 100, 1)
            causes.append(
                RootCause(
                    category="data_quality",
                    message=f"Outlier spike: {pct}% of rows flagged",
                    severity="medium",
                    evidence=d,
                )
            )
        elif check.name == "volume_anomaly":
            sign = "+" if d["change_pct"] >= 0 else ""
            causes.append(
                RootCause(
                    category="volume",
                    message=f"Data volume anomaly ({sign}{d['change_pct']}%)",
                    severity="high",
                    evidence=d,
                )
            )
        elif check.name == "invalid_ranges":
            cols = ", ".join(d["violations_by_column"].keys())
            causes.append(
                RootCause(
                    category="data_quality",
                    message=f"Invalid value ranges in: {cols}",
                    severity="medium",
                    evidence=d,
                )
            )
    return causes


def analyze(
    exception: Exception | None = None,
    validation_report: ValidationReport | None = None,
) -> RCAReport:
    causes: list[RootCause] = []

    if isinstance(exception, ExtractionError):
        causes.append(classify_extraction_error(exception))
    elif exception is not None:
        causes.append(
            RootCause(category="unknown", message=str(exception), severity="critical", evidence={})
        )

    if validation_report is not None:
        causes.extend(classify_validation_report(validation_report))

    return RCAReport(causes=causes)
