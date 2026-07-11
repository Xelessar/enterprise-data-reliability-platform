-- Warehouse schema for the Enterprise Data Reliability & Intelligence Platform.
-- Applied once at stack startup (see docker/postgres-init or scripts/bootstrap.sh).

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    source          TEXT,
    status          TEXT NOT NULL DEFAULT 'running', -- running | success | failed
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_seconds NUMERIC,
    row_count       INTEGER,
    dq_score        NUMERIC,
    anomaly_count   INTEGER,
    rca_summary     TEXT,
    UNIQUE (dag_id, run_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);

CREATE TABLE IF NOT EXISTS dq_results (
    id            BIGSERIAL PRIMARY KEY,
    pipeline_run_id BIGINT NOT NULL REFERENCES pipeline_runs (id) ON DELETE CASCADE,
    check_name    TEXT NOT NULL,
    passed        BOOLEAN NOT NULL,
    weight        NUMERIC NOT NULL,
    details       JSONB,
    evaluated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ml_anomalies (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_run_id BIGINT NOT NULL REFERENCES pipeline_runs (id) ON DELETE CASCADE,
    record_id       TEXT,
    anomaly_score   NUMERIC,
    is_anomaly      BOOLEAN NOT NULL,
    reason          TEXT,   -- human-readable "why" (largest-deviation feature)
    severity        TEXT,   -- critical | high | medium
    mlflow_run_id   TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_records (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT,
    payload     JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Columns match the source schema declared in config/config.yaml
-- (validation.required_columns: id, timestamp, value). `row_id` is the
-- table's own surrogate key so it never collides with the source `id`,
-- which is a business key, not guaranteed unique across pipeline runs.
CREATE TABLE IF NOT EXISTS processed_records (
    row_id          BIGSERIAL PRIMARY KEY,
    id              BIGINT,
    "timestamp"     TIMESTAMPTZ,
    value           NUMERIC,
    pipeline_run_id BIGINT REFERENCES pipeline_runs (id) ON DELETE SET NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processed_records_pipeline_run_id ON processed_records (pipeline_run_id);
