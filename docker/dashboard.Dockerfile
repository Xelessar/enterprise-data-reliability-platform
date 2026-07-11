FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.37.0 pandas==2.2.2 sqlalchemy==2.0.31 psycopg2-binary==2.9.9 \
    plotly==5.23.0 pyyaml==6.0.1 python-dotenv==1.0.1

COPY dashboard/ ./dashboard/
COPY monitoring/ ./monitoring/
COPY validation/ ./validation/
COPY etl/ ./etl/
COPY config/ ./config/

ENV PYTHONPATH=/app

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
