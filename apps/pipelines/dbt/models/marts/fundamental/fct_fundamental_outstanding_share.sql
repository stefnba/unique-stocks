WITH outstanding_share AS (
    SELECT *
    FROM {{ ref('stg_fundamental_stock_outstanding_shares') }}
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
            "outstanding_share.data_provider",
            "outstanding_share.provider_exchange_code",
            "outstanding_share.provider_instrument_code",
            "outstanding_share.snapshot_date",
            "outstanding_share.period_type",
            "outstanding_share.provider_period_label",
        ]) }} AS fundamental_outstanding_share_pk,
        instrument.instrument_pk,
        outstanding_share.data_provider,
        outstanding_share.provider_exchange_code,
        outstanding_share.provider_instrument_code,
        outstanding_share.snapshot_date,
        outstanding_share.period_type,
        outstanding_share.provider_period_label,
        outstanding_share.period_end_date,
        outstanding_share.shares_mln,
        outstanding_share.shares,
        outstanding_share.ingested_at
    FROM outstanding_share
    LEFT JOIN instrument
        ON outstanding_share.data_provider = instrument.data_provider
        AND outstanding_share.provider_exchange_code = instrument.provider_exchange_code
        AND outstanding_share.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
