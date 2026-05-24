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

CREATE TABLE IF NOT EXISTS bronze.eod_prices (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    bar_date DATE NOT NULL,
    open DECIMAL NOT NULL,
    high DECIMAL NOT NULL,
    low DECIMAL NOT NULL,
    close DECIMAL NOT NULL,
    volume BIGINT NOT NULL,
    adjusted_close DECIMAL,
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, bar_date, provider)
);

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

CREATE TABLE IF NOT EXISTS bronze.exchange_schedules (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    exchange_code VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    timezone VARCHAR NOT NULL,
    session_open VARCHAR NOT NULL,
    session_close VARCHAR NOT NULL,
    working_days VARCHAR NOT NULL,
    pre_market_open VARCHAR,
    pre_market_close VARCHAR,
    after_hours_open VARCHAR,
    after_hours_close VARCHAR,
    lunch_break_start VARCHAR,
    lunch_break_end VARCHAR,
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, exchange_code, provider)
);

CREATE TABLE IF NOT EXISTS bronze.exchange_holidays (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    exchange_code VARCHAR NOT NULL,
    holiday_date DATE NOT NULL,
    holiday_name VARCHAR NOT NULL,
    holiday_type VARCHAR NOT NULL,
    early_close_time VARCHAR,
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, exchange_code, holiday_date, provider)
);

CREATE TABLE IF NOT EXISTS bronze.instruments (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    country VARCHAR,
    exchange VARCHAR NOT NULL,
    currency VARCHAR,
    asset_type VARCHAR,
    isin VARCHAR,
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, exchange_code, ticker, provider)
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
