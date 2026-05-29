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

CREATE TABLE IF NOT EXISTS bronze.eod_price (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    bar_date DATE NOT NULL,
    open DECIMAL NOT NULL,
    high DECIMAL NOT NULL,
    low DECIMAL NOT NULL,
    close DECIMAL NOT NULL,
    volume BIGINT NOT NULL,
    adjusted_close DECIMAL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, bar_date, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.exchange_catalog (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    operating_mic_codes VARCHAR,
    country VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    country_iso2 VARCHAR NOT NULL,
    country_iso3 VARCHAR NOT NULL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, provider_exchange_code, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.exchange_mic_registry (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    mic VARCHAR NOT NULL,
    operating_mic VARCHAR NOT NULL,
    mic_type VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    legal_entity_name VARCHAR,
    lei VARCHAR,
    market_category_code VARCHAR,
    acronym VARCHAR,
    country_iso2 VARCHAR NOT NULL,
    city VARCHAR NOT NULL,
    website VARCHAR,
    status VARCHAR NOT NULL,
    creation_date DATE NOT NULL,
    last_update_date DATE,
    last_validation_date DATE,
    expiry_date DATE,
    comments VARCHAR,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, mic, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.exchange_schedule (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_schedule_exchange_code VARCHAR NOT NULL,
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
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, provider_schedule_exchange_code, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.exchange_holiday (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_schedule_exchange_code VARCHAR NOT NULL,
    holiday_date DATE NOT NULL,
    holiday_name VARCHAR NOT NULL,
    holiday_type VARCHAR NOT NULL,
    early_close_time VARCHAR,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, provider_schedule_exchange_code, holiday_date, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.instrument (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    country VARCHAR,
    provider_listing_exchange_code VARCHAR,
    currency VARCHAR,
    asset_type VARCHAR,
    isin VARCHAR,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, provider_exchange_code, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_document (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    code VARCHAR NOT NULL,
    name VARCHAR,
    instrument_type VARCHAR NOT NULL,
    instrument_family VARCHAR NOT NULL,
    primary_ticker VARCHAR,
    provider_listing_exchange_code VARCHAR,
    provider_updated_at DATE,
    top_level_sections JSON NOT NULL,
    payload_hash VARCHAR NOT NULL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_identity (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    code VARCHAR NOT NULL,
    name VARCHAR,
    primary_ticker VARCHAR,
    provider_listing_exchange_code VARCHAR,
    currency_code VARCHAR,
    currency_name VARCHAR,
    country_name VARCHAR,
    country_iso VARCHAR,
    isin VARCHAR,
    cusip VARCHAR,
    cik VARCHAR,
    lei VARCHAR,
    open_figi VARCHAR,
    employer_id_number VARCHAR,
    fiscal_year_end VARCHAR,
    ipo_date DATE,
    sector VARCHAR,
    industry VARCHAR,
    gic_sector VARCHAR,
    gic_group VARCHAR,
    gic_industry VARCHAR,
    gic_sub_industry VARCHAR,
    home_category VARCHAR,
    is_delisted BOOLEAN,
    delisted_date DATE,
    full_time_employees BIGINT,
    web_url VARCHAR,
    logo_url VARCHAR,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_statement_fact (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    statement_type VARCHAR NOT NULL,
    period_type VARCHAR NOT NULL,
    period_end_date DATE NOT NULL,
    filing_date DATE,
    currency_symbol VARCHAR,
    metric_name VARCHAR NOT NULL,
    metric_value DECIMAL NOT NULL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, statement_type, period_type, period_end_date, metric_name, data_provider)
);

-- -----------------------------------------------------------------------
-- Pipeline run tracking
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline.runs (
    run_id UUID DEFAULT GEN_RANDOM_UUID(),
    parent_run_id UUID,
    prefect_flow_run_id UUID,
    flow_name VARCHAR NOT NULL,
    domain VARCHAR NOT NULL,
    run_kind VARCHAR NOT NULL,
    provider VARCHAR,
    environment VARCHAR,
    code_version VARCHAR,
    parameters_json JSON,
    target_window_start DATE,
    target_window_end DATE,
    status VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    units_total INTEGER,
    units_succeeded INTEGER,
    units_failed INTEGER,
    units_skipped INTEGER,
    rows_raw INTEGER,
    rows_valid INTEGER,
    rows_rejected INTEGER,
    rows_written INTEGER,
    summary_json JSON,
    error_class VARCHAR,
    error_message TEXT,
    UNIQUE (run_id)
);

CREATE TABLE IF NOT EXISTS pipeline.run_units (
    unit_id UUID DEFAULT GEN_RANDOM_UUID(),
    run_id UUID NOT NULL,
    domain VARCHAR NOT NULL,
    provider VARCHAR,
    unit_type VARCHAR NOT NULL,
    unit_key_hash VARCHAR NOT NULL,
    unit_key_json JSON NOT NULL,
    status VARCHAR NOT NULL,
    reason VARCHAR,
    source_uri VARCHAR,
    rows_raw INTEGER,
    rows_valid INTEGER,
    rows_rejected INTEGER,
    rows_written INTEGER,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_class VARCHAR,
    error_message TEXT,
    UNIQUE (run_id, unit_type, unit_key_hash)
);

CREATE TABLE IF NOT EXISTS pipeline.landing_objects (
    landing_id UUID DEFAULT GEN_RANDOM_UUID(),
    run_id UUID NOT NULL,
    unit_id UUID,
    domain VARCHAR NOT NULL,
    provider VARCHAR,
    dataset VARCHAR NOT NULL,
    source_uri VARCHAR NOT NULL,
    partition_json JSON,
    rows_raw INTEGER,
    byte_count INTEGER,
    content_hash VARCHAR,
    recorded_at TIMESTAMPTZ NOT NULL,
    UNIQUE (run_id, source_uri)
);

CREATE TABLE IF NOT EXISTS pipeline.rejections (
    rejection_id UUID DEFAULT GEN_RANDOM_UUID(),
    run_id UUID NOT NULL,
    unit_id UUID,
    domain VARCHAR NOT NULL,
    entity_key_json JSON,
    source_uri VARCHAR,
    raw_hash VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    error_class VARCHAR,
    error_message TEXT,
    raw_sample_json JSON,
    recorded_at TIMESTAMPTZ NOT NULL,
    UNIQUE (run_id, domain, raw_hash)
);

CREATE TABLE IF NOT EXISTS pipeline.dbt_invocations (
    dbt_run_id UUID DEFAULT GEN_RANDOM_UUID(),
    run_id UUID NOT NULL,
    dbt_invocation_id UUID,
    command VARCHAR NOT NULL,
    command_args_json JSON NOT NULL,
    project_dir VARCHAR NOT NULL,
    profiles_dir VARCHAR NOT NULL,
    target VARCHAR,
    status VARCHAR NOT NULL,
    return_code INTEGER,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    elapsed_seconds DOUBLE,
    artifact_path VARCHAR,
    artifact_metadata_json JSON,
    error_message TEXT,
    UNIQUE (dbt_run_id)
);

CREATE TABLE IF NOT EXISTS pipeline.dbt_node_results (
    node_result_id UUID DEFAULT GEN_RANDOM_UUID(),
    dbt_run_id UUID NOT NULL,
    unique_id VARCHAR NOT NULL,
    resource_type VARCHAR,
    status VARCHAR NOT NULL,
    execution_time DOUBLE,
    failures INTEGER,
    message TEXT,
    adapter_response_json JSON,
    rows_affected INTEGER,
    relation_name VARCHAR,
    compiled BOOLEAN,
    UNIQUE (dbt_run_id, unique_id)
);
