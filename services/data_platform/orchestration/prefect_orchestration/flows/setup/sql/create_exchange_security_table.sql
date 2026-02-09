CREATE TABLE IF NOT EXISTS ingestion.exchange_security (
    "Code" VARCHAR,
    "Name" VARCHAR,
    "Country" VARCHAR,
    "Exchange" VARCHAR,
    "Currency" VARCHAR,
    "Type" VARCHAR,
    "Isin" VARCHAR,
    "ingested_at" TIMESTAMP,
    "exchange_code" VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/ingest/exchange_security/',
    partitioned_by = ARRAY ['ingested_at', 'exchange_code']
)
