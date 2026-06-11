SELECT
    invocation.dbt_run_id,
    invocation.command,
    invocation.target,
    invocation.return_code,
    invocation.elapsed_seconds AS invocation_elapsed_seconds,
    node.unique_id,
    node.resource_type,
    node.status,
    node.execution_time,
    node.failures,
    node.rows_affected,
    node.relation_name,
    node.message
FROM pipeline.dbt_invocations AS invocation
INNER JOIN pipeline.dbt_node_results AS node
    ON invocation.dbt_run_id = node.dbt_run_id
WHERE invocation.run_id = {{ param() }}
ORDER BY
    CASE node.status
        WHEN 'error' THEN 1
        WHEN 'fail' THEN 2
        WHEN 'warn' THEN 3
        ELSE 4
    END ASC,
    node.execution_time DESC NULLS LAST,
    node.unique_id ASC
LIMIT {{ param() }}
