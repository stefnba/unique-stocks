{{ config(tags=['ingestion_control']) }}

WITH provider_exchange AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_code_kind,
        source_kind,
        exchange_catalog_name,
        universe_tier,
        daily_coverage_mode,
        historical_coverage_mode,
        policy_priority,
        policy_reason,
        policy_owner,
        policy_last_reviewed_on
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

calendar_coverage_override AS (
    SELECT
        LOWER(TRIM(CAST(data_provider AS VARCHAR))) AS data_provider,
        UPPER(TRIM(CAST(provider_schedule_exchange_code AS VARCHAR))) AS provider_schedule_exchange_code,
        CAST(coverage_start_date AS DATE) AS coverage_start_date,
        CAST(coverage_end_date AS DATE) AS coverage_end_date,
        LOWER(TRIM(CAST(coverage_confidence AS VARCHAR))) AS coverage_confidence,
        NULLIF(TRIM(CAST(source_name AS VARCHAR)), '') AS source_name,
        NULLIF(TRIM(CAST(source_url AS VARCHAR)), '') AS source_url,
        CAST(reviewed_on AS DATE) AS reviewed_on
    FROM {{ ref('exchange_calendar_coverage_overrides') }}
    WHERE NULLIF(TRIM(CAST(data_provider AS VARCHAR)), '') IS NOT NULL
        AND NULLIF(TRIM(CAST(provider_schedule_exchange_code AS VARCHAR)), '') IS NOT NULL
        AND coverage_start_date IS NOT NULL
        AND coverage_end_date IS NOT NULL
        AND LOWER(TRIM(CAST(coverage_confidence AS VARCHAR))) IN ('high', 'medium', 'low')
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

schedule_coverage_override_bounds AS (
    SELECT
        data_provider,
        provider_schedule_exchange_code,
        MIN(coverage_start_date) AS override_coverage_start_date,
        MAX(coverage_end_date) AS override_coverage_end_date,
        CASE MAX(
            CASE coverage_confidence
                WHEN 'low' THEN 3
                WHEN 'medium' THEN 2
                WHEN 'high' THEN 1
            END
        )
            WHEN 1 THEN 'high'
            WHEN 2 THEN 'medium'
            WHEN 3 THEN 'low'
        END AS override_coverage_confidence,
        STRING_AGG(DISTINCT source_name, ',') AS override_coverage_source_names,
        STRING_AGG(DISTINCT source_url, ',') AS override_coverage_source_urls,
        MAX(reviewed_on) AS override_coverage_last_reviewed_on
    FROM calendar_coverage_override
    GROUP BY 1, 2
),

schedule_calendar_coverage_bounds AS (
    SELECT
        COALESCE(provider_holiday.data_provider, override.data_provider) AS data_provider,
        COALESCE(
            provider_holiday.provider_schedule_exchange_code,
            override.provider_schedule_exchange_code
        ) AS provider_schedule_exchange_code,
        LEAST(
            COALESCE(provider_holiday.holiday_coverage_start_date, override.override_coverage_start_date),
            COALESCE(override.override_coverage_start_date, provider_holiday.holiday_coverage_start_date)
        ) AS calendar_coverage_start_date,
        GREATEST(
            COALESCE(provider_holiday.holiday_coverage_end_date, override.override_coverage_end_date),
            COALESCE(override.override_coverage_end_date, provider_holiday.holiday_coverage_end_date)
        ) AS calendar_coverage_end_date,
        provider_holiday.holiday_coverage_start_date,
        provider_holiday.holiday_coverage_end_date,
        override.override_coverage_start_date,
        override.override_coverage_end_date,
        override.override_coverage_confidence,
        override.override_coverage_source_names,
        override.override_coverage_source_urls,
        override.override_coverage_last_reviewed_on
    FROM schedule_holiday_bounds AS provider_holiday
    FULL OUTER JOIN schedule_coverage_override_bounds AS override
        ON provider_holiday.data_provider = override.data_provider
        AND provider_holiday.provider_schedule_exchange_code = override.provider_schedule_exchange_code
),

calendar_coverage_window AS (
    SELECT
        data_provider,
        provider_schedule_exchange_code,
        holiday_coverage_start_date AS coverage_start_date,
        holiday_coverage_end_date AS coverage_end_date,
        'provider_holiday_calendar' AS coverage_source
    FROM schedule_holiday_bounds
    WHERE holiday_coverage_start_date IS NOT NULL
        AND holiday_coverage_end_date IS NOT NULL

    UNION ALL

    SELECT
        data_provider,
        provider_schedule_exchange_code,
        coverage_start_date,
        coverage_end_date,
        'reviewed_override' AS coverage_source
    FROM calendar_coverage_override
),

calendar_coverage_bounds AS (
    SELECT MAX(calendar_coverage_end_date) AS max_calendar_coverage_date
    FROM schedule_calendar_coverage_bounds
),

provider_exchange_bounds AS (
    SELECT
        provider_exchange.data_provider,
        provider_exchange.provider_exchange_code,
        provider_exchange.provider_code_kind,
        provider_exchange.source_kind,
        provider_exchange.exchange_catalog_name,
        provider_exchange.universe_tier,
        provider_exchange.daily_coverage_mode,
        provider_exchange.historical_coverage_mode,
        provider_exchange.policy_priority,
        provider_exchange.policy_reason,
        provider_exchange.policy_owner,
        provider_exchange.policy_last_reviewed_on,
        mic_mapping.mic,
        mic_mapping.mic_mapping_method,
        mic_mapping.mic_mapping_confidence,
        exchange_universe.creation_date AS exchange_creation_date,
        schedule_coverage.calendar_coverage_start_date,
        schedule_coverage.calendar_coverage_end_date,
        schedule_coverage.holiday_coverage_start_date,
        schedule_coverage.holiday_coverage_end_date,
        schedule_coverage.override_coverage_start_date,
        schedule_coverage.override_coverage_end_date,
        schedule_coverage.override_coverage_confidence,
        schedule_coverage.override_coverage_source_names,
        schedule_coverage.override_coverage_source_urls,
        schedule_coverage.override_coverage_last_reviewed_on,
        CASE
            WHEN exchange_universe.creation_date IS NOT NULL THEN exchange_universe.creation_date
            WHEN schedule_coverage.calendar_coverage_start_date <= CURRENT_DATE
                THEN schedule_coverage.calendar_coverage_start_date
            ELSE CURRENT_DATE
        END AS calendar_start_date,
        CASE
            WHEN exchange_universe.creation_date IS NOT NULL THEN 'mic_creation_date'
            WHEN schedule_coverage.override_coverage_start_date <= COALESCE(
                    schedule_coverage.holiday_coverage_start_date,
                    schedule_coverage.override_coverage_start_date
                )
                AND schedule_coverage.override_coverage_start_date <= CURRENT_DATE
                THEN 'reviewed_calendar_coverage'
            WHEN schedule_coverage.holiday_coverage_start_date <= CURRENT_DATE
                THEN 'provider_holiday_calendar'
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
    LEFT JOIN schedule_calendar_coverage_bounds AS schedule_coverage
        ON schedule_mapping.data_provider = schedule_coverage.data_provider
        AND schedule_mapping.provider_schedule_exchange_code = schedule_coverage.provider_schedule_exchange_code
),

date_bounds AS (
    SELECT
        COALESCE(MIN(provider_exchange_bounds.calendar_start_date), CURRENT_DATE) AS min_bar_date,
        GREATEST(
            CAST(DATE_TRUNC('year', CURRENT_DATE) + INTERVAL 2 YEAR - INTERVAL 1 DAY AS DATE),
            COALESCE(MAX(calendar_coverage_bounds.max_calendar_coverage_date), CURRENT_DATE)
        ) AS max_bar_date
    FROM provider_exchange_bounds
    CROSS JOIN calendar_coverage_bounds
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

calendar_coverage_date AS (
    SELECT
        coverage_window.data_provider,
        coverage_window.provider_schedule_exchange_code,
        date_spine.bar_date,
        CASE
            WHEN BOOL_OR(coverage_window.coverage_source = 'provider_holiday_calendar')
                AND BOOL_OR(coverage_window.coverage_source = 'reviewed_override')
                THEN 'provider_holiday_calendar_and_reviewed_override'
            WHEN BOOL_OR(coverage_window.coverage_source = 'reviewed_override') THEN 'reviewed_override'
            ELSE 'provider_holiday_calendar'
        END AS calendar_date_coverage_source
    FROM calendar_coverage_window AS coverage_window
    INNER JOIN date_spine
        ON date_spine.bar_date BETWEEN coverage_window.coverage_start_date AND coverage_window.coverage_end_date
    GROUP BY 1, 2, 3
),

joined AS (
    SELECT
        provider_exchange.data_provider,
        provider_exchange.provider_exchange_code,
        provider_exchange.provider_code_kind,
        provider_exchange.source_kind,
        provider_exchange.exchange_catalog_name,
        provider_exchange.universe_tier,
        provider_exchange.daily_coverage_mode,
        provider_exchange.historical_coverage_mode,
        provider_exchange.policy_priority,
        provider_exchange.policy_reason,
        provider_exchange.policy_owner,
        provider_exchange.policy_last_reviewed_on,
        provider_exchange.mic,
        provider_exchange.mic_mapping_method,
        provider_exchange.mic_mapping_confidence,
        provider_exchange.exchange_creation_date,
        provider_exchange.calendar_coverage_start_date,
        provider_exchange.calendar_coverage_end_date,
        provider_exchange.holiday_coverage_start_date,
        provider_exchange.holiday_coverage_end_date,
        provider_exchange.override_coverage_start_date,
        provider_exchange.override_coverage_end_date,
        provider_exchange.override_coverage_confidence,
        provider_exchange.override_coverage_source_names,
        provider_exchange.override_coverage_source_urls,
        provider_exchange.override_coverage_last_reviewed_on,
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
        AND calendar_coverage_date.bar_date IS NOT NULL
            AS is_calendar_coverage_known,
        COALESCE(calendar_coverage_date.calendar_date_coverage_source, CASE
            WHEN provider_exchange.override_coverage_start_date IS NOT NULL
                AND provider_exchange.holiday_coverage_start_date IS NOT NULL
                THEN 'provider_holiday_calendar_and_reviewed_override'
            WHEN provider_exchange.override_coverage_start_date IS NOT NULL THEN 'reviewed_override'
            WHEN provider_exchange.holiday_coverage_start_date IS NOT NULL THEN 'provider_holiday_calendar'
            ELSE 'none'
        END) AS calendar_coverage_source,
        CASE
            WHEN calendar.provider_schedule_exchange_code IS NULL THEN 'unknown_schedule_mapping'
            WHEN provider_exchange.calendar_coverage_start_date IS NULL THEN 'unknown_holiday_coverage'
            WHEN date_spine.bar_date < provider_exchange.calendar_coverage_start_date
                THEN 'before_holiday_coverage'
            WHEN date_spine.bar_date > provider_exchange.calendar_coverage_end_date THEN 'after_holiday_coverage'
            WHEN calendar_coverage_date.bar_date IS NULL THEN 'holiday_coverage_gap'
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
    LEFT JOIN calendar_coverage_date
        ON calendar.data_provider = calendar_coverage_date.data_provider
        AND calendar.provider_schedule_exchange_code = calendar_coverage_date.provider_schedule_exchange_code
        AND date_spine.bar_date = calendar_coverage_date.bar_date
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
    universe_tier,
    daily_coverage_mode,
    historical_coverage_mode,
    daily_coverage_mode = 'blocking' AS is_daily_coverage_blocking,
    policy_priority,
    policy_reason,
    policy_owner,
    policy_last_reviewed_on,
    mic,
    mic_mapping_method,
    mic_mapping_confidence,
    exchange_creation_date,
    calendar_coverage_start_date,
    calendar_coverage_end_date,
    holiday_coverage_start_date,
    holiday_coverage_end_date,
    override_coverage_start_date,
    override_coverage_end_date,
    override_coverage_confidence,
    override_coverage_source_names,
    override_coverage_source_urls,
    override_coverage_last_reviewed_on,
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
    calendar_coverage_source,
    calendar_coverage_status,
    is_working_day,
    is_full_holiday,
    is_early_close,
    is_calendar_known
    AND is_calendar_coverage_known
    AND is_working_day
    AND NOT is_full_holiday AS is_trading_day,
    full_holiday_name,
    early_close_name,
    early_close_time
FROM joined
