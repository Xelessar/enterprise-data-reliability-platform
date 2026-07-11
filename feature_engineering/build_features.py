"""Turn a validated, transformed DataFrame into a numeric feature matrix for
the anomaly detection model. Kept separate from `ml/` so features can be
reused by future models without touching training code.
"""
from __future__ import annotations

import pandas as pd


def build_feature_matrix(df: pd.DataFrame, feature_columns: list[str] | None = None) -> pd.DataFrame:
    numeric_df = df.select_dtypes(include="number")
    if feature_columns:
        available = [c for c in feature_columns if c in numeric_df.columns]
        numeric_df = numeric_df[available] if available else numeric_df

    if numeric_df.empty:
        raise ValueError("No numeric columns available to build a feature matrix from")

    return numeric_df.fillna(numeric_df.median(numeric_only=True))
