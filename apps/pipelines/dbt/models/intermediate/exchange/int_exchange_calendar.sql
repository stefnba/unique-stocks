WITH latest_schedule AS (
    SELECT *
    FROM {{ ref('stg_exchange_schedule') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data_provider, provider_schedule_exchange_code
            ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
        ) = 1
),

provider_links AS (
    SELECT
        code_mapping.data_provider,
        code_mapping.mapping_code AS provider_schedule_exchange_code,
        STRING_AGG(DISTINCT code_mapping.provider_exchange_code, ',') AS linked_provider_exchange_codes
    FROM {{ ref('int_provider_code_mapping') }} AS code_mapping
    INNER JOIN {{ ref('int_exchange_provider_ingestion_universe') }} AS provider_universe
        ON code_mapping.data_provider = provider_universe.data_provider
        AND code_mapping.provider_exchange_code = provider_universe.provider_exchange_code
    WHERE code_mapping.mapping_type = 'schedule_code'
        AND provider_universe.is_enabled_for_eod_price
    GROUP BY 1, 2
)

SELECT
    latest_schedule.data_provider || ':' || latest_schedule.provider_schedule_exchange_code AS exchange_calendar_id,
    latest_schedule.data_provider,
    latest_schedule.provider_schedule_exchange_code,
    provider_links.linked_provider_exchange_codes,
    latest_schedule.exchange_schedule_name,
    latest_schedule.timezone,
    latest_schedule.session_open,
    latest_schedule.session_close,
    latest_schedule.working_days,
    latest_schedule.pre_market_open,
    latest_schedule.pre_market_close,
    latest_schedule.after_hours_open,
    latest_schedule.after_hours_close,
    latest_schedule.lunch_break_start,
    latest_schedule.lunch_break_end,
    latest_schedule.has_pre_market,
    latest_schedule.has_after_hours,
    latest_schedule.has_lunch_break,
    latest_schedule.snapshot_date AS schedule_snapshot_date,
    latest_schedule.row_hash AS schedule_row_hash,
    latest_schedule.source_uri AS schedule_source_uri,
    latest_schedule.ingested_at AS schedule_ingested_at
FROM latest_schedule
INNER JOIN provider_links
    ON latest_schedule.data_provider = provider_links.data_provider
    AND latest_schedule.provider_schedule_exchange_code = provider_links.provider_schedule_exchange_code
