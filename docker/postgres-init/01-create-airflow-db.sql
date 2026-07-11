-- Runs once against the default POSTGRES_DB on first container start.
-- Airflow's metadata store lives in its own database, separate from the
-- application warehouse tables created by scripts/init_db.sql.
CREATE DATABASE airflow;
