-- Initialise unique_stocks schemas and tables.
-- Safe to run multiple times (all statements are idempotent).
-- Run against local DuckDB:  duckdb unique_stocks.db < apps/pipelines/scripts/init_db.sql
-- Run against MotherDuck:    MOTHERDUCK_TOKEN=... duckdb md:unique_stocks < apps/pipelines/scripts/init_db.sql

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS pipeline;

-- -----------------------------------------------------------------------
-- Bronze — raw, immutable, append-only
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.eod_prices (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    ticker VARCHAR NOT NULL,
    bar_date DATE NOT NULL,
    provider VARCHAR NOT NULL,  -- 'eodhd'
    raw_json JSON NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    row_hash VARCHAR NOT NULL   -- SHA-256 of canonical payload
);

CREATE TABLE IF NOT EXISTS bronze.securities (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    exchange VARCHAR NOT NULL,
    snapshot_date DATE NOT NULL,
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.exchanges (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.fundamentals (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    ticker VARCHAR NOT NULL,
    fiscal_year INTEGER,
    fiscal_period VARCHAR,                -- 'Q1', 'Q2', 'Q3', 'Q4', 'TTM'
    report_type VARCHAR,                -- 'income_statement', 'balance_sheet', 'cash_flow'
    provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- Pipeline run tracking
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline.runs (
    run_id UUID DEFAULT GEN_RANDOM_UUID(),
    flow_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,  -- 'running' | 'completed' | 'failed' | 'skipped'
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    rows_written INTEGER,
    error_message TEXT
);
