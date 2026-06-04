WITH exchange_universe AS (
    SELECT *
    FROM {{ ref('int_exchange_universe') }}
),

final AS (
    SELECT
        {{ surrogate_key(["mic"]) }} AS exchange_pk,
        mic,
        operating_mic,
        mic_type,
        exchange_name,
        legal_entity_name,
        lei,
        market_category_code,
        acronym,
        country_iso2,
        city,
        website,
        status,
        comments,
        is_operating_mic,
        is_active,
        registry_provider,
        creation_date,
        last_update_date,
        last_validation_date,
        expiry_date,
        registry_snapshot_date,
        registry_ingested_at
    FROM exchange_universe
)

SELECT * FROM final
