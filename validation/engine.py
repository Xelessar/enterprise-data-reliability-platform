"""Data Validation Engine: runs configured checks and produces a single
Data Quality Score plus a structured issue list the RCA engine consumes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from validation.checks import (
    CheckResult,
    check_dtypes,
    check_duplicates,
    check_invalid_ranges,
    check_null_percentage,
    check_outliers_iqr,
    check_required_columns,
    check_volume_anomaly,
)


@dataclass
class ValidationReport:
    dq_score: float
    passed: bool
    checks: list[CheckResult]
    row_count: int
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def failing_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dq_score": self.dq_score,
            "passed": self.passed,
            "row_count": self.row_count,
            "evaluated_at": self.evaluated_at.isoformat(),
            "checks": [
                {"name": c.name, "passed": c.passed, "weight": c.weight, "details": c.details}
                for c in self.checks
            ],
        }


class DataValidationEngine:
    def __init__(self, validation_config: dict):
        self.config = validation_config

    def run(self, df: pd.DataFrame, baseline_row_count: int | None = None) -> ValidationReport:
        cfg = self.config
        checks: list[CheckResult] = [
            check_required_columns(df, cfg.get("required_columns", [])),
            check_duplicates(df, cfg.get("duplicate_pk_columns", [])),
            check_null_percentage(
                df, warn_pct=cfg.get("null_pct_warn", 5), fail_pct=cfg.get("null_pct_fail", 20)
            ),
            check_outliers_iqr(df, multiplier=cfg.get("outlier_iqr_multiplier", 1.5)),
            check_volume_anomaly(
                current_row_count=len(df),
                baseline_row_count=baseline_row_count,
                threshold_pct=cfg.get("volume_anomaly_pct_threshold", 50),
            ),
        ]

        if cfg.get("expected_dtypes"):
            checks.append(check_dtypes(df, cfg["expected_dtypes"]))
        if cfg.get("range_rules"):
            checks.append(check_invalid_ranges(df, cfg["range_rules"]))

        dq_score = self._compute_score(checks)
        threshold = cfg.get("dq_score_fail_threshold", 70)

        return ValidationReport(
            dq_score=dq_score,
            passed=dq_score >= threshold,
            checks=checks,
            row_count=len(df),
        )

    @staticmethod
    def _compute_score(checks: list[CheckResult]) -> float:
        total_weight = sum(c.weight for c in checks) or 1.0
        earned = sum(c.weight for c in checks if c.passed)
        return round((earned / total_weight) * 100, 2)
