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

calendar AS (
    SELECT *
    FROM {{ ref('int_exchange_calendar') }}
),

holiday AS (
    SELECT *
    FROM {{ ref('int_exchange_holiday_calendar') }}
),

price_bounds AS (
    SELECT MIN(bar_date) AS min_bar_date
    FROM {{ ref('stg_eod_price') }}
),

date_bounds AS (
    SELECT
        COALESCE(min_bar_date, CURRENT_DATE) AS min_bar_date,
        CURRENT_DATE AS max_bar_date
    FROM price_bounds
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
        AND POSITION(',' || date_spine.day_name || ',' IN ',' || calendar.working_days || ',') > 0
            AS is_working_day,
        full_holiday.holiday_date IS NOT NULL AS is_full_holiday,
        early_close.holiday_date IS NOT NULL AS is_early_close,
        full_holiday.holiday_name AS full_holiday_name,
        early_close.holiday_name AS early_close_name,
        early_close.early_close_time
    FROM provider_exchange
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
)

SELECT
    data_provider || ':' || provider_exchange_code || ':' || CAST(bar_date AS VARCHAR) AS exchange_trading_day_id,
    data_provider,
    provider_exchange_code,
    provider_code_kind,
    source_kind,
    exchange_catalog_name,
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
    is_calendar_known AND is_working_day AND NOT is_full_holiday AS is_trading_day,
    full_holiday_name,
    early_close_name,
    early_close_time
FROM joined
