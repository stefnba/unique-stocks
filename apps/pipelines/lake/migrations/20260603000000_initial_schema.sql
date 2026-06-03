-- Initial unique_stocks lake schema.
-- This is the first canonical lake schema migration.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE SCHEMA IF NOT EXISTS lake;

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

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_earnings_fact (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    earnings_section VARCHAR NOT NULL,
    period_type VARCHAR,
    fiscal_period_end DATE NOT NULL,
    report_date DATE,
    before_after_market VARCHAR,
    currency_code VARCHAR,
    fiscal_quarter VARCHAR,
    period_offset VARCHAR,
    metric_name VARCHAR NOT NULL,
    metric_value DECIMAL NOT NULL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, earnings_section, period_type, fiscal_period_end, metric_name, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_shares_stats (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    shares_outstanding DECIMAL,
    shares_float DECIMAL,
    percent_insiders DECIMAL,
    percent_institutions DECIMAL,
    shares_short DECIMAL,
    shares_short_prior_month DECIMAL,
    short_ratio DECIMAL,
    short_percent_outstanding DECIMAL,
    short_percent_float DECIMAL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_outstanding_shares (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    period_type VARCHAR NOT NULL,
    provider_period_label VARCHAR NOT NULL,
    period_end_date DATE NOT NULL,
    shares_mln DECIMAL,
    shares DECIMAL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, period_type, period_end_date, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_holder (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    holder_type VARCHAR NOT NULL,
    provider_position BIGINT,
    holder_name VARCHAR NOT NULL,
    report_date DATE NOT NULL,
    total_shares_percent DECIMAL,
    total_assets_percent DECIMAL,
    current_shares DECIMAL,
    shares_change DECIMAL,
    shares_change_percent DECIMAL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, holder_type, holder_name, report_date, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_insider_transaction (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    provider_position BIGINT,
    filing_date DATE,
    owner_cik VARCHAR,
    owner_name VARCHAR NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_code VARCHAR NOT NULL,
    transaction_amount DECIMAL,
    transaction_price DECIMAL,
    transaction_acquired_disposed VARCHAR,
    post_transaction_amount DECIMAL,
    sec_link VARCHAR,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, provider_position, owner_name, transaction_date, transaction_code, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_splits_dividends (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    forward_annual_dividend_rate DECIMAL,
    forward_annual_dividend_yield DECIMAL,
    payout_ratio DECIMAL,
    dividend_date DATE,
    ex_dividend_date DATE,
    last_split_factor VARCHAR,
    last_split_date DATE,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_dividend_count (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    year BIGINT NOT NULL,
    dividend_count BIGINT NOT NULL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, year, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_metric_fact (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    metric_group VARCHAR NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value DECIMAL NOT NULL,
    metric_date DATE,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, metric_group, metric_name, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_stock_esg_activity (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    rating_date DATE,
    activity VARCHAR NOT NULL,
    involvement VARCHAR,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, activity, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_etf_identity (
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
    open_figi VARCHAR,
    company_name VARCHAR,
    company_url VARCHAR,
    etf_url VARCHAR,
    domicile VARCHAR,
    index_name VARCHAR,
    inception_date DATE,
    dividend_paying_frequency VARCHAR,
    holdings_count BIGINT,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_mutual_fund_identity (
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
    open_figi VARCHAR,
    fund_category VARCHAR,
    fund_family VARCHAR,
    fund_style VARCHAR,
    fiscal_year_end VARCHAR,
    domicile VARCHAR,
    inception_date DATE,
    update_date DATE,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_index_identity (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    code VARCHAR NOT NULL,
    name VARCHAR,
    provider_listing_exchange_code VARCHAR,
    currency_code VARCHAR,
    currency_name VARCHAR,
    country_name VARCHAR,
    country_iso VARCHAR,
    open_figi VARCHAR,
    market_cap DECIMAL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_etf_holding (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    holding_symbol VARCHAR NOT NULL,
    holding_code VARCHAR,
    holding_exchange VARCHAR,
    holding_name VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    country VARCHAR,
    region VARCHAR,
    assets_percent DECIMAL,
    is_top_10 BOOLEAN NOT NULL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, holding_symbol, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_mutual_fund_holding (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    provider_position BIGINT,
    holding_name VARCHAR NOT NULL,
    weight_percent DECIMAL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, provider_position, holding_name, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_fund_metric_fact (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    instrument_family VARCHAR NOT NULL,
    metric_group VARCHAR NOT NULL,
    metric_category VARCHAR,
    metric_name VARCHAR NOT NULL,
    metric_value DECIMAL NOT NULL,
    metric_date DATE,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, instrument_family, metric_group, metric_category, metric_name, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_index_component (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    provider_position BIGINT,
    component_code VARCHAR NOT NULL,
    component_exchange VARCHAR,
    component_ticker VARCHAR,
    component_name VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    weight DECIMAL,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, component_code, component_exchange, data_provider)
);

CREATE TABLE IF NOT EXISTS bronze.fundamental_index_historical_component (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    provider_position BIGINT,
    component_code VARCHAR NOT NULL,
    component_name VARCHAR,
    start_date DATE,
    end_date DATE,
    is_active_now BOOLEAN,
    is_delisted BOOLEAN,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, ticker, component_code, start_date, data_provider)
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

CREATE TABLE IF NOT EXISTS pipeline.ingestion_coverage (
    coverage_id UUID DEFAULT GEN_RANDOM_UUID(),
    run_id UUID NOT NULL,
    domain VARCHAR NOT NULL,
    provider VARCHAR NOT NULL,
    unit_type VARCHAR NOT NULL,
    unit_key_hash VARCHAR NOT NULL,
    unit_key_json JSON NOT NULL,
    status VARCHAR NOT NULL,
    reason VARCHAR,
    rows_raw INTEGER,
    rows_valid INTEGER,
    rows_rejected INTEGER,
    source_uri VARCHAR,
    recorded_at TIMESTAMPTZ NOT NULL,
    UNIQUE (domain, provider, unit_type, unit_key_hash, status)
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
