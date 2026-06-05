SELECT no_data_coverage.*
FROM {{ ref('int_eod_price_backfill_no_data_coverage') }} AS no_data_coverage
INNER JOIN {{ ref('stg_pipeline_ingestion_coverage') }} AS coverage
    ON no_data_coverage.data_provider = coverage.data_provider
    AND no_data_coverage.unit_key_hash = coverage.unit_key_hash
    AND coverage.status = 'no_data'
WHERE coverage.domain != 'eod_price'
   OR coverage.unit_type != 'ticker_backfill'
