SELECT
    COUNT(*) AS dbt_invocations,
    COUNT(*) FILTER (
        WHERE invocation.status NOT IN ('completed', 'success', 'pass')
    ) AS dbt_attention_invocations
FROM pipeline.dbt_invocations AS invocation
INNER JOIN pipeline.runs AS run
    ON invocation.run_id = run.run_id
WHERE {{ where_clauses | default("TRUE") }}
