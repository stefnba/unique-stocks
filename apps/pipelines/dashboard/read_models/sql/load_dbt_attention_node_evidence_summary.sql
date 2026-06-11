SELECT COUNT(*) AS dbt_attention_nodes
FROM pipeline.dbt_invocations AS invocation
INNER JOIN pipeline.dbt_node_results AS node
    ON invocation.dbt_run_id = node.dbt_run_id
INNER JOIN pipeline.runs AS run
    ON invocation.run_id = run.run_id
WHERE {{ where_clauses | default("TRUE") }}
    AND node.status IN ('error', 'fail', 'warn')
