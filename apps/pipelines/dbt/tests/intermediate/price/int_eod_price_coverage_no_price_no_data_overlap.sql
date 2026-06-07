SELECT *
FROM {{ ref('int_eod_price_instrument_day_coverage') }}
WHERE has_price
    AND has_no_data_coverage
