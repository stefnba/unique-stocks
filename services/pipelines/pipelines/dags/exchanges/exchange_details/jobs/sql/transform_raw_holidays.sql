SELECT
    holidays.exchange_source_code AS exchange_source_code,
    exchange_codes.uid AS exchange_uid,
    holidays.Holiday AS holiday_name,
    holidays.Type AS holiday_type,
    holiday_types.uid AS holiday_type_id,
    holidays.Date AS date
FROM
    $holidays_data AS holidays
    LEFT JOIN $mappings_data AS exchange_codes ON holidays.exchange_source_code = exchange_codes.source_value
        AND exchange_codes.product = 'exchange'
        AND exchange_codes.field = 'source_code'
    LEFT JOIN $mappings_data AS holiday_types ON holidays.Type = holiday_types.source_value
        AND holiday_types.product = 'exchange'
        AND holiday_types.field = 'holiday_type'
