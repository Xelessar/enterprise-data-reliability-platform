"""Individual, independently-testable data quality checks.

Every check returns a CheckResult so the engine can aggregate them into a
single Data Quality Score and the Root Cause Analysis engine can consume the
`details` dict directly without re-deriving anything.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class CheckResult:
    name: str
    passed: bool
    weight: float
    details: dict[str, Any] = field(default_factory=dict)


def check_required_columns(
    df: pd.DataFrame, required_columns: list[str], weight: float = 20.0
) -> CheckResult:
    missing = [c for c in required_columns if c not in df.columns]
    return CheckResult(
        name="required_columns",
        passed=not missing,
        weight=weight,
        details={"missing_columns": missing},
    )


def check_duplicates(df: pd.DataFrame, pk_columns: list[str], weight: float = 15.0) -> CheckResult:
    pk_columns = [c for c in pk_columns if c in df.columns]
    if not pk_columns:
        return CheckResult(name="duplicates", passed=True, weight=weight, details={"pk_columns_found": False})
    dup_count = int(df.duplicated(subset=pk_columns).sum())
    return CheckResult(
        name="duplicates",
        passed=dup_count == 0,
        weight=weight,
        details={"duplicate_count": dup_count, "pk_columns": pk_columns},
    )


def check_null_percentage(
    df: pd.DataFrame, warn_pct: float = 5.0, fail_pct: float = 20.0, weight: float = 20.0
) -> CheckResult:
    if len(df) == 0:
        return CheckResult(
            name="null_percentage", passed=False, weight=weight, details={"empty_dataframe": True}
        )
    null_pct = (df.isna().sum() / len(df) * 100).round(2)
    offending = null_pct[null_pct >= fail_pct]
    warning = null_pct[(null_pct >= warn_pct) & (null_pct < fail_pct)]
    return CheckResult(
        name="null_percentage",
        passed=offending.empty,
        weight=weight,
        details={
            "null_pct_by_column": null_pct.to_dict(),
            "failing_columns": offending.to_dict(),
            "warning_columns": warning.to_dict(),
        },
    )


def check_dtypes(df: pd.DataFrame, expected_dtypes: dict[str, str], weight: float = 10.0) -> CheckResult:
    mismatches = {}
    for col, expected in expected_dtypes.items():
        if col not in df.columns:
            continue
        actual = str(df[col].dtype)
        if expected not in actual:
            mismatches[col] = {"expected": expected, "actual": actual}
    return CheckResult(
        name="dtypes",
        passed=not mismatches,
        weight=weight,
        details={"mismatches": mismatches},
    )


def check_outliers_iqr(
    df: pd.DataFrame, numeric_columns: list[str] | None = None, multiplier: float = 1.5, weight: float = 15.0
) -> CheckResult:
    numeric_columns = numeric_columns or df.select_dtypes(include=np.number).columns.tolist()
    outlier_counts: dict[str, int] = {}
    for col in numeric_columns:
        if col not in df.columns:
            continue
        series = df[col].dropna()
        if series.empty:
            continue
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        lower, upper = q1 - multiplier * iqr, q3 + multiplier * iqr
        count = int(((series < lower) | (series > upper)).sum())
        if count:
            outlier_counts[col] = count
    total_outliers = sum(outlier_counts.values())
    outlier_ratio = total_outliers / len(df) if len(df) else 0
    return CheckResult(
        name="outliers",
        passed=outlier_ratio < 0.10,
        weight=weight,
        details={"outlier_counts_by_column": outlier_counts, "outlier_ratio": round(outlier_ratio, 4)},
    )


def check_volume_anomaly(
    current_row_count: int, baseline_row_count: int | None, threshold_pct: float = 50.0, weight: float = 10.0
) -> CheckResult:
    if not baseline_row_count:
        return CheckResult(
            name="volume_anomaly", passed=True, weight=weight, details={"baseline_available": False}
        )
    change_pct = ((current_row_count - baseline_row_count) / baseline_row_count) * 100
    return CheckResult(
        name="volume_anomaly",
        passed=abs(change_pct) < threshold_pct,
        weight=weight,
        details={
            "current_row_count": current_row_count,
            "baseline_row_count": baseline_row_count,
            "change_pct": round(change_pct, 2),
        },
    )


def check_invalid_ranges(
    df: pd.DataFrame, range_rules: dict[str, tuple[float, float]], weight: float = 10.0
) -> CheckResult:
    violations: dict[str, int] = {}
    for col, (low, high) in range_rules.items():
        if col not in df.columns:
            continue
        count = int((~df[col].between(low, high) & df[col].notna()).sum())
        if count:
            violations[col] = count
    return CheckResult(
        name="invalid_ranges",
        passed=not violations,
        weight=weight,
        details={"violations_by_column": violations},
    )
