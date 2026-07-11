import pandas as pd

from etl.extract import ExtractionError
from monitoring.rca import analyze
from validation.engine import DataValidationEngine


def test_extraction_error_produces_readable_cause():
    exc = ExtractionError("API timeout: https://api.example.com/orders", reason_code="api_timeout", url="https://api.example.com/orders")
    report = analyze(exception=exc)
    assert len(report.causes) == 1
    assert "API timeout" in report.causes[0].message
    assert "Pipeline failed" in report.summary
    assert "- API timeout" in report.summary


def test_validation_failures_produce_multiple_ranked_causes():
    df = pd.DataFrame({"id": [1, 1, 2], "timestamp": pd.date_range("2026-01-01", periods=3, freq="h")})
    config = {
        "required_columns": ["id", "timestamp", "value"],
        "duplicate_pk_columns": ["id"],
        "null_pct_warn": 5,
        "null_pct_fail": 20,
        "dq_score_fail_threshold": 70,
    }
    validation_report = DataValidationEngine(config).run(df)
    assert validation_report.passed is False

    rca_report = analyze(validation_report=validation_report)
    messages = [c.message for c in rca_report.causes]

    assert any("Missing column" in m for m in messages)
    assert any("Duplicate primary keys" in m for m in messages)
    # critical (schema) causes must be ranked above high/medium ones
    assert rca_report.causes
    ordered_summary_lines = rca_report.summary.splitlines()[3:]
    assert ordered_summary_lines[0].startswith("- Missing column")


def test_no_signal_yields_unknown_reason():
    report = analyze()
    assert report.causes == []
    assert "unknown" in report.summary.lower()
