FROM python:3.11-slim

RUN pip install --no-cache-dir mlflow==2.14.3 psycopg2-binary==2.9.9

EXPOSE 5000

ENTRYPOINT ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
