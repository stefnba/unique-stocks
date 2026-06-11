SELECT COUNT(*) AS coverage_records
FROM pipeline.ingestion_coverage
WHERE {{ where_clauses }}
