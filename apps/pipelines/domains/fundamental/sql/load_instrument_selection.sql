SELECT
    universe.provider_exchange_code,
    universe.provider_instrument_code
FROM {{ universe_relation }} AS universe
WHERE universe.data_provider = {{ param() }}
    {% if exchange_filter %}
        AND universe.provider_exchange_code IN ({{ exchange_placeholders }})
    {% endif %}
    {% if skip_completed %}
        AND NOT EXISTS (
            SELECT 1
            FROM {{ completion_relation }} AS completed_document
            WHERE completed_document.data_provider = universe.data_provider
                AND completed_document.provider_exchange_code = universe.provider_exchange_code
                AND completed_document.provider_instrument_code = universe.provider_instrument_code
                AND completed_document.snapshot_date = {{ param() }}
        )
    {% endif %}
ORDER BY universe.provider_exchange_code, universe.provider_instrument_code
{% if limit_filter %}
    LIMIT {{ param() }}
{% endif %}
