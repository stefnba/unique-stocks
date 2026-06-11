{{ config(tags=['ingestion_control']) }}

WITH provider_exchange AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_code_kind,
        source_kind,
        exchange_catalog_name
    FROM {{ ref('int_exchange_provider_ingestion_universe') }}
    WHERE data_provider = 'eodhd'
        AND is_enabled_for_eod_price
),

schedule_mapping AS (
    SELECT
        data_provider,
        provider_exchange_code,
        mapping_code AS provider_schedule_exchange_code,
        mapping_method,
        mapping_confidence
    FROM {{ ref('int_provider_code_mapping') }}
    WHERE mapping_type = 'schedule_code'
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data_provider, provider_exchange_code
            ORDER BY
                CASE mapping_confidence
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    ELSE 3
                END,
                mapping_method,
                mapping_code
        ) = 1
),

mic_mapping AS (
    SELECT
        data_provider,
        provider_exchange_code,
        mapping_code AS mic,
        mapping_method AS mic_mapping_method,
        mapping_confidence AS mic_mapping_confidence
    FROM {{ ref('int_provider_code_mapping') }}
    WHERE mapping_type = 'mic'
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data_provider, provider_exchange_code
            ORDER BY
                CASE mapping_confidence
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    ELSE 3
                END,
                mapping_method ASC,
                mapping_code ASC
        ) = 1
),

exchange_universe AS (
    SELECT *
    FROM {{ ref('int_exchange_universe') }}
),

calendar AS (
    SELECT *
    FROM {{ ref('int_exchange_calendar') }}
),

holiday AS (
    SELECT *
    FROM {{ ref('int_exchange_holiday_calendar') }}
),

schedule_holiday_bounds AS (
    SELECT
        data_provider,
        provider_schedule_exchange_code,
        MIN(holiday_date) AS holiday_coverage_start_date,
        MAX(holiday_date) AS holiday_coverage_end_date
    FROM holiday
    GROUP BY 1, 2
),

holiday_bounds AS (
    SELECT MAX(holiday_date) AS max_holiday_date
    FROM holiday
),

provider_exchange_bounds AS (
    SELECT
        provider_exchange.data_provider,
        provider_exchange.provider_exchange_code,
        provider_exchange.provider_code_kind,
        provider_exchange.source_kind,
        provider_exchange.exchange_catalog_name,
        mic_mapping.mic,
        mic_mapping.mic_mapping_method,
        mic_mapping.mic_mapping_confidence,
        exchange_universe.creation_date AS exchange_creation_date,
        schedule_holiday_bounds.holiday_coverage_start_date,
        schedule_holiday_bounds.holiday_coverage_end_date,
        CASE
            WHEN exchange_universe.creation_date IS NOT NULL THEN exchange_universe.creation_date
            WHEN schedule_holiday_bounds.holiday_coverage_start_date <= CURRENT_DATE
                THEN schedule_holiday_bounds.holiday_coverage_start_date
            ELSE CURRENT_DATE
        END AS calendar_start_date,
        CASE
            WHEN exchange_universe.creation_date IS NOT NULL THEN 'mic_creation_date'
            WHEN schedule_holiday_bounds.holiday_coverage_start_date <= CURRENT_DATE THEN 'provider_holiday_calendar'
            ELSE 'current_date_default'
        END AS calendar_start_date_source
    FROM provider_exchange
    LEFT JOIN schedule_mapping
        ON provider_exchange.data_provider = schedule_mapping.data_provider
        AND provider_exchange.provider_exchange_code = schedule_mapping.provider_exchange_code
    LEFT JOIN mic_mapping
        ON provider_exchange.data_provider = mic_mapping.data_provider
        AND provider_exchange.provider_exchange_code = mic_mapping.provider_exchange_code
    LEFT JOIN exchange_universe
        ON mic_mapping.mic = exchange_universe.mic
    LEFT JOIN schedule_holiday_bounds
        ON schedule_mapping.data_provider = schedule_holiday_bounds.data_provider
        AND schedule_mapping.provider_schedule_exchange_code = schedule_holiday_bounds.provider_schedule_exchange_code
),

date_bounds AS (
    SELECT
        COALESCE(MIN(provider_exchange_bounds.calendar_start_date), CURRENT_DATE) AS min_bar_date,
        GREATEST(
            CAST(DATE_TRUNC('year', CURRENT_DATE) + INTERVAL 2 YEAR - INTERVAL 1 DAY AS DATE),
            COALESCE(MAX(holiday_bounds.max_holiday_date), CURRENT_DATE)
        ) AS max_bar_date
    FROM provider_exchange_bounds
    CROSS JOIN holiday_bounds
),

date_spine AS (
    SELECT
        CAST(series_dates.generated_date AS DATE) AS bar_date,
        STRFTIME(CAST(series_dates.generated_date AS DATE), '%a') AS day_name
    FROM date_bounds
    CROSS JOIN GENERATE_SERIES(
        date_bounds.min_bar_date,
        date_bounds.max_bar_date,
        INTERVAL 1 DAY
    ) AS series_dates (generated_date)
),

joined AS (
    SELECT
        provider_exchange.data_provider,
        provider_exchange.provider_exchange_code,
        provider_exchange.provider_code_kind,
        provider_exchange.source_kind,
        provider_exchange.exchange_catalog_name,
        provider_exchange.mic,
        provider_exchange.mic_mapping_method,
        provider_exchange.mic_mapping_confidence,
        provider_exchange.exchange_creation_date,
        provider_exchange.holiday_coverage_start_date AS calendar_coverage_start_date,
        provider_exchange.holiday_coverage_end_date AS calendar_coverage_end_date,
        provider_exchange.calendar_start_date,
        provider_exchange.calendar_start_date_source,
        schedule_mapping.provider_schedule_exchange_code,
        schedule_mapping.mapping_method AS schedule_mapping_method,
        schedule_mapping.mapping_confidence AS schedule_mapping_confidence,
        calendar.exchange_schedule_name,
        calendar.timezone,
        calendar.working_days,
        date_spine.bar_date,
        date_spine.day_name,
        calendar.provider_schedule_exchange_code IS NOT NULL AS is_calendar_known,
        calendar.provider_schedule_exchange_code IS NOT NULL
        AND provider_exchange.holiday_coverage_start_date IS NOT NULL
        AND date_spine.bar_date BETWEEN
        provider_exchange.holiday_coverage_start_date AND provider_exchange.holiday_coverage_end_date
            AS is_calendar_coverage_known,
        CASE
            WHEN calendar.provider_schedule_exchange_code IS NULL THEN 'unknown_schedule_mapping'
            WHEN provider_exchange.holiday_coverage_start_date IS NULL THEN 'unknown_holiday_coverage'
            WHEN date_spine.bar_date < provider_exchange.holiday_coverage_start_date THEN 'before_holiday_coverage'
            WHEN date_spine.bar_date > provider_exchange.holiday_coverage_end_date THEN 'after_holiday_coverage'
            ELSE 'covered'
        END AS calendar_coverage_status,
        calendar.provider_schedule_exchange_code IS NOT NULL
        AND POSITION(',' || date_spine.day_name || ',' IN ',' || calendar.working_days || ',') > 0
            AS is_working_day,
        full_holiday.holiday_date IS NOT NULL AS is_full_holiday,
        early_close.holiday_date IS NOT NULL AS is_early_close,
        full_holiday.holiday_name AS full_holiday_name,
        early_close.holiday_name AS early_close_name,
        early_close.early_close_time
    FROM provider_exchange_bounds AS provider_exchange
    CROSS JOIN date_spine
    LEFT JOIN schedule_mapping
        ON provider_exchange.data_provider = schedule_mapping.data_provider
        AND provider_exchange.provider_exchange_code = schedule_mapping.provider_exchange_code
    LEFT JOIN calendar
        ON schedule_mapping.data_provider = calendar.data_provider
        AND schedule_mapping.provider_schedule_exchange_code = calendar.provider_schedule_exchange_code
    LEFT JOIN holiday AS full_holiday
        ON calendar.data_provider = full_holiday.data_provider
        AND calendar.provider_schedule_exchange_code = full_holiday.provider_schedule_exchange_code
        AND date_spine.bar_date = full_holiday.holiday_date
        AND NOT full_holiday.is_early_close
    LEFT JOIN holiday AS early_close
        ON calendar.data_provider = early_close.data_provider
        AND calendar.provider_schedule_exchange_code = early_close.provider_schedule_exchange_code
        AND date_spine.bar_date = early_close.holiday_date
        AND early_close.is_early_close
    WHERE date_spine.bar_date >= provider_exchange.calendar_start_date
)

SELECT
    data_provider || ':' || provider_exchange_code || ':' || CAST(bar_date AS VARCHAR) AS exchange_trading_day_id,
    data_provider,
    provider_exchange_code,
    provider_code_kind,
    source_kind,
    exchange_catalog_name,
    mic,
    mic_mapping_method,
    mic_mapping_confidence,
    exchange_creation_date,
    calendar_coverage_start_date,
    calendar_coverage_end_date,
    calendar_start_date,
    calendar_start_date_source,
    provider_schedule_exchange_code,
    schedule_mapping_method,
    schedule_mapping_confidence,
    exchange_schedule_name,
    timezone,
    working_days,
    bar_date,
    day_name,
    is_calendar_known,
    is_calendar_coverage_known,
    calendar_coverage_status,
    is_working_day,
    is_full_holiday,
    is_early_close,
    is_calendar_known AND is_working_day AND NOT is_full_holiday AS is_trading_day,
    full_holiday_name,
    early_close_name,
    early_close_time
FROM joined
