SELECT
    holidays_df.*,
    mappings_df.uid
FROM
    $holidays_df AS holidays_df
    LEFT JOIN $mappings_df AS mappings_df ON holidays_df.exchange_code = mappings_df.source_value
