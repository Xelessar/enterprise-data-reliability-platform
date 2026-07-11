"""Load transformed data into the warehouse (Postgres now, BigQuery-ready later)."""
from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def load_to_postgres(
    df: pd.DataFrame, table_name: str, engine: Engine, if_exists: str = "append", schema: str = "public"
) -> int:
    df.to_sql(
        table_name, engine, schema=schema, if_exists=if_exists, index=False, method="multi", chunksize=1000
    )
    logger.info("Loaded %d rows into %s.%s", len(df), schema, table_name)
    return len(df)


def load_to_bigquery(df: pd.DataFrame, table_id: str, project_id: str, if_exists: str = "append") -> int:
    """Optional cloud connector. Only imports google-cloud-bigquery when actually called."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND" if if_exists == "append" else "WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows into BigQuery table %s", len(df), table_id)
    return len(df)
