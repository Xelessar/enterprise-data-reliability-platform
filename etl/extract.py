"""Source connectors. Each extractor returns a pandas DataFrame plus lightweight
extraction metadata (row_count, source, extracted_at) used downstream by the
validation engine and the Root Cause Analysis engine.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Raised when a source cannot be read. Caught by the DAG and handed to the RCA engine."""

    def __init__(self, message: str, reason_code: str, **context: Any):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = context


@dataclass
class ExtractionResult:
    df: pd.DataFrame
    source: str
    extracted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def row_count(self) -> int:
        return len(self.df)


def extract_csv(path: str | Path) -> ExtractionResult:
    path = Path(path)
    if not path.exists():
        raise ExtractionError(
            f"CSV source not found: {path}", reason_code="missing_source_file", path=str(path)
        )
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError as exc:
        raise ExtractionError(
            f"CSV source is empty: {path}", reason_code="empty_source", path=str(path)
        ) from exc
    logger.info("Extracted %d rows from CSV %s", len(df), path)
    return ExtractionResult(df=df, source=f"csv:{path.name}")


def extract_rest_api(
    base_url: str, endpoint: str, api_key: str | None = None, timeout: int = 30, params: dict | None = None
) -> ExtractionResult:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
    except requests.Timeout as exc:
        raise ExtractionError(f"API timeout: {url}", reason_code="api_timeout", url=url) from exc
    except requests.HTTPError as exc:
        raise ExtractionError(
            f"API returned {resp.status_code}: {url}",
            reason_code="api_error_status",
            url=url,
            status_code=resp.status_code,
        ) from exc

    payload = resp.json()
    records = payload.get("data", payload) if isinstance(payload, dict) else payload
    df = pd.DataFrame(records)
    logger.info("Extracted %d rows from API %s", len(df), url)
    return ExtractionResult(df=df, source=f"api:{endpoint}")


def extract_postgres(query: str, engine: Engine) -> ExtractionResult:
    try:
        df = pd.read_sql(query, engine)
    except Exception as exc:  # noqa: BLE001 - surfaced via ExtractionError with context
        raise ExtractionError(
            f"Postgres query failed: {exc}", reason_code="db_query_failed", query=query
        ) from exc
    logger.info("Extracted %d rows from Postgres", len(df))
    return ExtractionResult(df=df, source="postgres")


EXTRACTORS = {
    "csv": extract_csv,
    "rest_api": extract_rest_api,
    "postgres": extract_postgres,
}


def extract(source_type: str, **kwargs: Any) -> ExtractionResult:
    if source_type not in EXTRACTORS:
        raise ValueError(f"Unknown source_type '{source_type}'. Available: {list(EXTRACTORS)}")
    return EXTRACTORS[source_type](**kwargs)
