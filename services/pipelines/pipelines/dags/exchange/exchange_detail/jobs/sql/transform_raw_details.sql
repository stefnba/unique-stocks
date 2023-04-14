SELECT
    exchange_codes.uid AS exchange_uid,
    details.Code AS exchange_source_code,
    details.Name AS exchange_name,
    countries.uid AS country,
    details.Currency AS currency,
    details.Timezone AS timezone,
    struct_pack (close_utc := strptime (details.TradingHours.CloseUTC, '%H:%M:%S'), open_utc := strptime (details.TradingHours.OpenUTC, '%H:%M:%S'), OPEN := strptime (details.TradingHours.Open, '%H:%M:%S'), CLOSE := strptime (details.TradingHours.Close, '%H:%M:%S')) AS trading_hours,
    string_split (details.TradingHours.WorkingDays, ',') AS working_days,
    details.ActiveTickers AS active_tickers,
    details.UpdatedTickers AS updated_tickers,
    details.PreviousDayUpdatedTickers AS previous_day_updated_tickers
FROM
    $details_data AS details
    LEFT JOIN $exchange_code_mapping AS exchange_codes ON details.Code = exchange_codes.source_value
    LEFT JOIN $country_mapping AS countries ON details.Country = countries.source_value
