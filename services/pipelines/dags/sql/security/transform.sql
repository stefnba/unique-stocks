SELECT
    "Code" AS "code",
    "Name" AS "name",
    "Isin" AS "isin",
    "Country" AS "country",
    "Currency" AS "currency",
    replace("Exchange", ' ', '_') AS "exchange_code",
    replace(lower("Type"), ' ', '_') AS "type",
FROM
    $security_raw
