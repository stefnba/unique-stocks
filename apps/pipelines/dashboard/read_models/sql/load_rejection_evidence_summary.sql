SELECT COUNT(*) AS rejection_samples
FROM pipeline.rejections
WHERE {{ where_clauses }}
