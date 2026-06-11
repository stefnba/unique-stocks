WITH statement AS (
    SELECT *
    FROM {{ ref('stg_fundamental_statement_fact') }}
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
            "statement.data_provider",
            "statement.provider_exchange_code",
            "statement.provider_instrument_code",
            "statement.snapshot_date",
            "statement.statement_type",
            "statement.period_type",
            "statement.period_end_date",
            "statement.metric_name",
        ]) }} AS fundamental_statement_pk,
        instrument.instrument_pk,
        statement.data_provider,
        statement.provider_exchange_code,
        statement.provider_instrument_code,
        statement.snapshot_date,
        statement.statement_type,
        statement.period_type,
        statement.period_end_date,
        statement.filing_date,
        statement.currency_symbol,
        statement.metric_name,
        statement.metric_value,
        statement.ingested_at
    FROM statement
    LEFT JOIN instrument
        ON statement.data_provider = instrument.data_provider
        AND statement.provider_exchange_code = instrument.provider_exchange_code
        AND statement.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
