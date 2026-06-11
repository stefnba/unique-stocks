WITH holder AS (
    SELECT *
    FROM {{ ref('stg_fundamental_stock_holder') }}
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
            "holder.data_provider",
            "holder.provider_exchange_code",
            "holder.provider_instrument_code",
            "holder.snapshot_date",
            "holder.holder_type",
            "holder.provider_position",
            "holder.holder_name",
            "holder.report_date",
        ]) }} AS fundamental_holder_pk,
        instrument.instrument_pk,
        holder.data_provider,
        holder.provider_exchange_code,
        holder.provider_instrument_code,
        holder.snapshot_date,
        holder.holder_type,
        holder.provider_position,
        holder.holder_name,
        holder.report_date,
        holder.total_shares_percent,
        holder.total_assets_percent,
        holder.current_shares,
        holder.shares_change,
        holder.shares_change_percent,
        holder.ingested_at
    FROM holder
    LEFT JOIN instrument
        ON holder.data_provider = instrument.data_provider
        AND holder.provider_exchange_code = instrument.provider_exchange_code
        AND holder.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
