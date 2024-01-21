SELECT
    Name AS name,
    Code AS code,
    OperatingMIC AS operating_mic,
    IF(Currency='Unknown', NULL, Currency) AS currency,
    IF(CountryISO2='', NULL, CountryISO2) AS country
FROM
    read_json('${exchange}', auto_detect=True)
