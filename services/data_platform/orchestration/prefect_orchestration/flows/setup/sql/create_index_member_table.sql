CREATE TABLE IF NOT EXISTS ingestion.index_member (
    "Index_Code" VARCHAR,
    "Index_Name" VARCHAR,
    "Code" VARCHAR,
    "Exchange" VARCHAR,
    "Name" VARCHAR,
    "Sector" VARCHAR,
    "Industry" VARCHAR,
    "Weight" DOUBLE,
    "ingested_at" TIMESTAMP
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/ingest/index_member/',
    partitioned_by = ARRAY ['ingested_at']
)
