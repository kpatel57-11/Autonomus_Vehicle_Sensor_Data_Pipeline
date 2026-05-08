-- scripts/init_postgres.sql
-- Runs at container startup to initialize the av_metadata database

CREATE SCHEMA IF NOT EXISTS av;

-- Dataset metadata table
CREATE TABLE IF NOT EXISTS dataset_metadata (
    id                SERIAL PRIMARY KEY,
    name              VARCHAR(128) UNIQUE NOT NULL,
    path              TEXT NOT NULL,
    format            VARCHAR(32),
    size_gb           INTEGER DEFAULT 0,
    record_count      BIGINT  DEFAULT 0,
    freshness_minutes INTEGER DEFAULT 60,
    schema_version    VARCHAR(16) DEFAULT '1',
    tags              TEXT[],
    created_at        TIMESTAMP DEFAULT NOW(),
    updated_at        TIMESTAMP DEFAULT NOW()
);

-- Pipeline run history
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  VARCHAR(64) PRIMARY KEY,
    recipe_name         VARCHAR(128),
    mode                VARCHAR(16) CHECK (mode IN ('batch','stream')),
    status              VARCHAR(32) CHECK (status IN ('running','success','failed','cancelled')),
    records_read        BIGINT DEFAULT 0,
    records_valid       BIGINT DEFAULT 0,
    records_processed   BIGINT DEFAULT 0,
    records_rejected    BIGINT DEFAULT 0,
    elapsed_s           FLOAT  DEFAULT 0,
    error_message       TEXT,
    started_at          TIMESTAMP DEFAULT NOW(),
    finished_at         TIMESTAMP
);

-- Checkpoint log for audit trail
CREATE TABLE IF NOT EXISTS checkpoint_log (
    id          SERIAL PRIMARY KEY,
    run_id      VARCHAR(64) REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    source      VARCHAR(128) NOT NULL,
    offset_json JSONB,
    saved_at    TIMESTAMP DEFAULT NOW()
);

-- Sensor schema registry mirror
CREATE TABLE IF NOT EXISTS sensor_schemas (
    id              SERIAL PRIMARY KEY,
    subject         VARCHAR(128) UNIQUE NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    schema_json     JSONB NOT NULL,
    registered_at   TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_runs_status    ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started   ON pipeline_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_checkpoint_src ON checkpoint_log(source);

-- Seed initial dataset entries
INSERT INTO dataset_metadata (name, path, format, size_gb, record_count, freshness_minutes, schema_version)
VALUES
    ('lidar_raw',       's3://av-sensor-data/lidar/',     'parquet', 2400, 300000000, 15,  '3'),
    ('camera_meta',     's3://av-sensor-data/camera/',    'parquet', 890,  80000000,  5,   '2'),
    ('gps_stream',      's3://av-sensor-data/gps/',       'delta',   45,   10000000,  1,   '4'),
    ('radar_points',    's3://av-sensor-data/radar/',     'orc',     320,  50000000,  30,  '1'),
    ('fused_perception','s3://av-sensor-data/fused/',     'hudi',    1100, 200000000, 30,  '5'),
    ('occupancy_grid',  's3://av-sensor-data/occupancy/', 'delta',   670,  120000000, 60,  '2')
ON CONFLICT (name) DO NOTHING;
