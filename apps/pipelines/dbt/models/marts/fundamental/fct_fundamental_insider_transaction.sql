WITH insider_transaction AS (
    SELECT *
    FROM {{ ref('stg_fundamental_stock_insider_transaction') }}
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
            "insider_transaction.data_provider",
            "insider_transaction.provider_exchange_code",
            "insider_transaction.provider_instrument_code",
            "insider_transaction.snapshot_date",
            "insider_transaction.provider_position",
            "insider_transaction.owner_cik",
            "insider_transaction.owner_name",
            "insider_transaction.transaction_date",
            "insider_transaction.transaction_code",
        ]) }} AS fundamental_insider_transaction_pk,
        instrument.instrument_pk,
        insider_transaction.data_provider,
        insider_transaction.provider_exchange_code,
        insider_transaction.provider_instrument_code,
        insider_transaction.snapshot_date,
        insider_transaction.provider_position,
        insider_transaction.filing_date,
        insider_transaction.owner_cik,
        insider_transaction.owner_name,
        insider_transaction.transaction_date,
        insider_transaction.transaction_code,
        insider_transaction.transaction_amount,
        insider_transaction.transaction_price,
        insider_transaction.transaction_acquired_disposed,
        insider_transaction.post_transaction_amount,
        insider_transaction.sec_link,
        insider_transaction.ingested_at
    FROM insider_transaction
    LEFT JOIN instrument
        ON insider_transaction.data_provider = instrument.data_provider
        AND insider_transaction.provider_exchange_code = instrument.provider_exchange_code
        AND insider_transaction.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
