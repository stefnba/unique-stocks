-- Initialise unique_stocks schemas and tables.
-- Safe to run multiple times (all statements are idempotent).
-- Generated from Python table specs. Do not edit by hand.
-- Regenerate with: uv run python scripts/render_init_lake_sql.py > scripts/init_lake.sql

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE SCHEMA IF NOT EXISTS pipeline;

-- -----------------------------------------------------------------------
-- Bronze - raw, immutable, append-only
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.exchanges (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    exchange_code VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    operating_mic VARCHAR,
    country VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    country_iso2 VARCHAR NOT NULL,
    country_iso3 VARCHAR NOT NULL,
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, exchange_code, provider)
);

-- -----------------------------------------------------------------------
-- Pipeline run tracking
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline.runs (
    run_id UUID DEFAULT GEN_RANDOM_UUID(),
    flow_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    rows_written INTEGER,
    error_message TEXT,
    UNIQUE (run_id)
);
