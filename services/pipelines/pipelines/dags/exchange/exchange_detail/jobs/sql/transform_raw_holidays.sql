SELECT
    holidays.exchange_source_code AS exchange_source_code,
    exchange_codes.uid AS exchange_uid,
    holidays.Holiday AS holiday_name,
    holidays.Type AS holiday_type,
    holiday_types.uid AS holiday_type_id,
    holidays.Date AS date
FROM
    $holidays_data AS holidays
    LEFT JOIN $exchange_codes_mapping AS exchange_codes ON holidays.exchange_source_code = exchange_codes.source_value
    LEFT JOIN $holiday_types_mapping AS holiday_types ON holidays.Type = holiday_types.source_value
