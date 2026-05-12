CREATE TABLE IF NOT EXISTS ingestion.security_quote (
    "date" DATE,
    "open" DOUBLE,
    "high" DOUBLE,
    "low" DOUBLE,
    "close" DOUBLE,
    "adjusted_close" DOUBLE,
    "volume" bigint,
    "ingested_at" TIMESTAMP,
    "exchange_code" VARCHAR,
    "security_code" VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/ingest/security_quote/',
    partitioned_by = ARRAY ['ingested_at', 'exchange_code', 'security_code']
)
