"""etl.load tests. load_to_bigquery is tested against a stubbed
google.cloud.bigquery module (injected via sys.modules) rather than the real
SDK -- google-cloud-bigquery lives only in requirements-gcp.txt, installed
in the Airflow container, and deliberately kept out of the shared local
.venv because its protobuf/google-api-core pins conflict with mlflow and
streamlit's (see docker/airflow.Dockerfile)."""
import sys
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest


class _FakeNotFound(Exception):
    pass


@pytest.fixture
def fake_bigquery(monkeypatch):
    """Builds a fake google.cloud.bigquery + google.cloud.exceptions module
    pair and registers them in sys.modules so `from google.cloud import
    bigquery` inside load_to_bigquery resolves to our fakes."""
    fake_client = MagicMock()
    fake_job = MagicMock()
    fake_client.load_table_from_dataframe.return_value = fake_job

    bigquery_module = types.SimpleNamespace(
        Client=MagicMock(return_value=fake_client),
        DatasetReference=lambda project, dataset: (project, dataset),
        Dataset=lambda ref: ref,
        LoadJobConfig=lambda write_disposition: types.SimpleNamespace(write_disposition=write_disposition),
    )
    exceptions_module = types.SimpleNamespace(NotFound=_FakeNotFound)

    google_module = sys.modules.get("google", types.ModuleType("google"))
    cloud_module = types.ModuleType("google.cloud")
    google_module.cloud = cloud_module
    cloud_module.bigquery = bigquery_module
    cloud_module.exceptions = exceptions_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)
    monkeypatch.setitem(sys.modules, "google.cloud.exceptions", exceptions_module)

    return {"client": fake_client, "job": fake_job}


def test_load_to_bigquery_creates_dataset_when_missing(fake_bigquery):
    from etl.load import load_to_bigquery

    fake_bigquery["client"].get_dataset.side_effect = _FakeNotFound()

    result = load_to_bigquery(
        pd.DataFrame({"id": [1, 2]}), dataset="analytics", table="processed_records", project_id="proj"
    )

    fake_bigquery["client"].create_dataset.assert_called_once()
    assert result == 2


def test_load_to_bigquery_skips_create_when_dataset_exists(fake_bigquery):
    from etl.load import load_to_bigquery

    load_to_bigquery(
        pd.DataFrame({"id": [1]}), dataset="analytics", table="processed_records", project_id="proj"
    )

    fake_bigquery["client"].create_dataset.assert_not_called()


def test_load_to_bigquery_uses_fully_qualified_table_id(fake_bigquery):
    from etl.load import load_to_bigquery

    load_to_bigquery(
        pd.DataFrame({"id": [1]}), dataset="analytics", table="processed_records", project_id="proj"
    )

    args, _ = fake_bigquery["client"].load_table_from_dataframe.call_args
    assert args[1] == "proj.analytics.processed_records"
