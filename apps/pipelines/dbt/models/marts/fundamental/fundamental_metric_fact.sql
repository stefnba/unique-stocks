WITH stock_metric AS (
    SELECT
        data_provider || ':' || ticker || ':' || CAST(snapshot_date AS VARCHAR)
        || ':stock:'
        || COALESCE(metric_group, 'unknown')
        || ':'
        || COALESCE(metric_name, 'unknown') AS fundamental_metric_fact_id,
        snapshot_date,
        provider_exchange_code,
        ticker,
        'stock' AS instrument_family,
        metric_group,
        CAST(NULL AS VARCHAR) AS metric_category,
        metric_name,
        metric_value,
        metric_date,
        data_provider,
        ingested_at
    FROM {{ ref('stg_fundamental_stock_metric_fact') }}
),

fund_metric AS (
    SELECT
        data_provider || ':' || ticker || ':' || CAST(snapshot_date AS VARCHAR)
        || ':' || COALESCE(instrument_family, 'fund')
        || ':' || COALESCE(metric_group, 'unknown')
        || ':' || COALESCE(metric_category, 'none')
        || ':' || COALESCE(metric_name, 'unknown') AS fundamental_metric_fact_id,
        snapshot_date,
        provider_exchange_code,
        ticker,
        instrument_family,
        metric_group,
        metric_category,
        metric_name,
        metric_value,
        metric_date,
        data_provider,
        ingested_at
    FROM {{ ref('stg_fundamental_fund_metric_fact') }}
)

SELECT * FROM stock_metric
UNION ALL
SELECT * FROM fund_metric
