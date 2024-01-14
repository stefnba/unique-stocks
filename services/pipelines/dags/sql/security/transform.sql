CREATE OR REPLACE TEMP TABLE "tbl" AS
SELECT
    *
FROM
    read_csv_auto('$securities/*/*.csv', HEADER=TRUE, hive_partitioning=0);

ALTER TABLE "tbl"
    ADD COLUMN IF NOT EXISTS "isin" TEXT;

SELECT
    "Code" AS "code",
    "Name" AS "name",
    "Isin" AS "isin",
    "Country" AS "country",
    "Currency" AS "currency",
    replace("Exchange", ' ', '_') AS "exchange_code",
    replace(lower("Type"), ' ', '_') AS "type",
FROM
    "tbl";

