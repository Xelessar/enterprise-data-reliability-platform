import pandas as pd
import pytest

from validation.engine import DataValidationEngine

BASE_CONFIG = {
    "required_columns": ["id", "timestamp", "value"],
    "duplicate_pk_columns": ["id"],
    "null_pct_warn": 5,
    "null_pct_fail": 20,
    "outlier_iqr_multiplier": 1.5,
    "dq_score_fail_threshold": 70,
}


def make_clean_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": range(1, 21),
            "timestamp": pd.date_range("2026-01-01", periods=20, freq="h"),
            "value": [10.0 + i for i in range(20)],
        }
    )


def test_clean_dataframe_scores_100():
    engine = DataValidationEngine(BASE_CONFIG)
    report = engine.run(make_clean_df())
    assert report.dq_score == 100.0
    assert report.passed is True
    assert report.failing_checks() == []


def test_missing_required_column_fails_check():
    df = make_clean_df().drop(columns=["value"])
    engine = DataValidationEngine(BASE_CONFIG)
    report = engine.run(df)
    failing_names = {c.name for c in report.failing_checks()}
    assert "required_columns" in failing_names
    assert report.dq_score < 100.0


def test_duplicate_primary_keys_detected():
    df = make_clean_df()
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)  # duplicate id=1
    engine = DataValidationEngine(BASE_CONFIG)
    report = engine.run(df)
    dup_check = next(c for c in report.checks if c.name == "duplicates")
    assert dup_check.passed is False
    assert dup_check.details["duplicate_count"] == 1


def test_high_null_percentage_fails_check():
    df = make_clean_df()
    df.loc[: int(len(df) * 0.5), "value"] = None
    engine = DataValidationEngine(BASE_CONFIG)
    report = engine.run(df)
    null_check = next(c for c in report.checks if c.name == "null_percentage")
    assert null_check.passed is False
    assert "value" in null_check.details["failing_columns"]


def test_volume_anomaly_flagged_against_baseline():
    df = make_clean_df()
    engine = DataValidationEngine(BASE_CONFIG)
    report = engine.run(df, baseline_row_count=1000)  # -98% vs baseline
    volume_check = next(c for c in report.checks if c.name == "volume_anomaly")
    assert volume_check.passed is False
    assert volume_check.details["change_pct"] < -50


@pytest.mark.parametrize("threshold", [0, 100])
def test_score_is_bounded_between_0_and_100(threshold):
    config = {**BASE_CONFIG, "dq_score_fail_threshold": threshold}
    engine = DataValidationEngine(config)
    report = engine.run(make_clean_df())
    assert 0.0 <= report.dq_score <= 100.0
