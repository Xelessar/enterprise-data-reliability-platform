FROM python:3.11-slim

RUN pip install --no-cache-dir mlflow==2.14.3 psycopg2-binary==2.9.9

EXPOSE 5000

# No ENTRYPOINT here — the full `mlflow server ...` invocation (including the
# backend-store-uri, which needs compose's env-var interpolation) is supplied
# entirely by docker-compose.yml's `command:` for this service.
