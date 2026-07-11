"""Reusable transformation steps. Kept pure (DataFrame in, DataFrame out) so they
can be unit tested without a database or Airflow context.
"""
from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    return df


def parse_timestamp(df: pd.DataFrame, column: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    if column in df.columns:
        df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)
    return df


def drop_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d exact-duplicate rows", dropped)
    return df


def cast_dtypes(df: pd.DataFrame, dtype_map: dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    for col, dtype in dtype_map.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError):
                logger.warning("Could not cast column %s to %s", col, dtype)
    return df


def transform(df: pd.DataFrame, dtype_map: dict[str, str] | None = None) -> pd.DataFrame:
    """Standard transform pipeline applied to every source before validation/load."""
    df = normalize_columns(df)
    df = strip_string_columns(df)
    df = parse_timestamp(df)
    df = drop_exact_duplicates(df)
    if dtype_map:
        df = cast_dtypes(df, dtype_map)
    return df
