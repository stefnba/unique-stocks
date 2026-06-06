{{ config(tags=['ingestion_control']) }}

WITH expected AS (
    SELECT *
    FROM {{ ref('int_eod_price_expected_instrument_day') }}
),

price AS (
    SELECT *
    FROM {{ ref('stg_eod_price') }}
),

no_data_coverage AS (
    SELECT *
    FROM {{ ref('int_eod_price_backfill_terminal_coverage') }}
    WHERE status = 'no_data'
),

joined AS (
    SELECT
        expected.expected_instrument_day_id,
        expected.data_provider,
        expected.provider_exchange_code,
        expected.provider_instrument_code,
        expected.instrument_family,
        expected.instrument_universe_id,
        expected.instrument_snapshot_date,
        expected.min_bar_date,
        expected.max_bar_date,
        expected.bar_count,
        expected.provider_schedule_exchange_code,
        expected.schedule_mapping_method,
        expected.schedule_mapping_confidence,
        expected.exchange_schedule_name,
        expected.timezone,
        expected.working_days,
        expected.bar_date,
        expected.day_name,
        expected.is_calendar_known,
        expected.is_working_day,
        expected.is_full_holiday,
        expected.is_early_close,
        expected.is_trading_day,
        price.ingestion_id AS price_ingestion_id,
        price.open_price,
        price.high_price,
        price.low_price,
        price.close_price,
        price.adjusted_close_price,
        price.volume,
        price.source_uri AS price_source_uri,
        price.ingested_at AS price_ingested_at,
        no_data_coverage.unit_key_hash AS no_data_unit_key_hash,
        no_data_coverage.reason AS no_data_reason,
        no_data_coverage.source_uri AS no_data_source_uri,
        no_data_coverage.recorded_at AS no_data_recorded_at
    FROM expected
    LEFT JOIN price
        ON expected.data_provider = price.data_provider
        AND expected.provider_exchange_code = price.provider_exchange_code
        AND expected.provider_instrument_code = price.provider_instrument_code
        AND expected.bar_date = price.bar_date
    LEFT JOIN no_data_coverage
        ON expected.data_provider = no_data_coverage.data_provider
        AND expected.provider_exchange_code = no_data_coverage.provider_exchange_code
        AND expected.provider_instrument_code = no_data_coverage.provider_instrument_code
        AND (
            no_data_coverage.from_date IS NULL
            OR expected.bar_date >= no_data_coverage.from_date
        )
        AND expected.bar_date <= no_data_coverage.to_date
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY expected.expected_instrument_day_id
            ORDER BY no_data_coverage.recorded_at DESC NULLS LAST, no_data_coverage.unit_key_hash ASC
        ) = 1
)

SELECT
    expected_instrument_day_id AS instrument_day_coverage_id,
    data_provider,
    provider_exchange_code,
    provider_instrument_code,
    instrument_family,
    instrument_universe_id,
    instrument_snapshot_date,
    min_bar_date,
    max_bar_date,
    bar_count,
    provider_schedule_exchange_code,
    schedule_mapping_method,
    schedule_mapping_confidence,
    exchange_schedule_name,
    timezone,
    working_days,
    bar_date,
    day_name,
    is_calendar_known,
    is_working_day,
    is_full_holiday,
    is_early_close,
    is_trading_day,
    price_ingestion_id IS NOT NULL AS has_price,
    no_data_unit_key_hash IS NOT NULL AS has_no_data_coverage,
    CASE
        WHEN NOT is_calendar_known THEN 'unknown_calendar'
        WHEN price_ingestion_id IS NOT NULL THEN 'priced'
        WHEN no_data_unit_key_hash IS NOT NULL THEN 'known_no_data'
        ELSE 'missing_price'
    END AS coverage_status,
    price_ingestion_id,
    open_price,
    high_price,
    low_price,
    close_price,
    adjusted_close_price,
    volume,
    price_source_uri,
    price_ingested_at,
    no_data_unit_key_hash,
    no_data_reason,
    no_data_source_uri,
    no_data_recorded_at
FROM joined
