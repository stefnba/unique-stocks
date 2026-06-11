WITH earnings AS (
    SELECT *
    FROM {{ ref('stg_fundamental_stock_earnings_fact') }}
),

instrument AS (
    SELECT
        instrument_pk,
        data_provider,
        provider_exchange_code,
        provider_instrument_code
    FROM {{ ref('dim_instrument_core') }}
),

final AS (
    SELECT
        {{ surrogate_key([
            "earnings.data_provider",
            "earnings.provider_exchange_code",
            "earnings.provider_instrument_code",
            "earnings.snapshot_date",
            "earnings.earnings_section",
            "earnings.fiscal_period_end",
            "earnings.metric_name",
            "earnings.period_offset",
        ]) }} AS fundamental_earnings_pk,
        instrument.instrument_pk,
        earnings.data_provider,
        earnings.provider_exchange_code,
        earnings.provider_instrument_code,
        earnings.snapshot_date,
        earnings.earnings_section,
        earnings.period_type,
        earnings.fiscal_period_end,
        earnings.report_date,
        earnings.before_after_market,
        earnings.currency_code,
        earnings.fiscal_quarter,
        earnings.period_offset,
        earnings.metric_name,
        earnings.metric_value,
        earnings.ingested_at
    FROM earnings
    LEFT JOIN instrument
        ON earnings.data_provider = instrument.data_provider
        AND earnings.provider_exchange_code = instrument.provider_exchange_code
        AND earnings.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
