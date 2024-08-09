CREATE TABLE IF NOT EXISTS ingestion.exchange (
    "Name" VARCHAR,
    "Code" VARCHAR,
    "OperatingMIC" VARCHAR,
    "Country" VARCHAR,
    "Currency" VARCHAR,
    "CountryISO2" VARCHAR,
    "CountryISO3" VARCHAR,
    "ingested_at" TIMESTAMP
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://lakehouse/ingest/exchanges/',
  partitioned_by = ARRAY['ingested_at']
)