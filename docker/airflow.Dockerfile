FROM apache/airflow:2.9.3-python3.11

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Installed as a separate layer/resolve: mixing google-cloud-bigquery's grpc
# chain into the same `pip install` as Airflow sends the resolver into
# multi-minute backtracking (see requirements-gcp.txt).
COPY requirements-gcp.txt /tmp/requirements-gcp.txt
RUN pip install --no-cache-dir -r /tmp/requirements-gcp.txt
