WITH stock_metric AS (
    SELECT
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
),

metric AS (
    SELECT * FROM stock_metric
    UNION ALL
    SELECT * FROM fund_metric
),

security AS (
    SELECT
        security_pk,
        data_provider,
        provider_symbol
    FROM {{ ref('dim_security') }}
),

final AS (
    SELECT
        {{ surrogate_key([
            "metric.data_provider",
            "metric.ticker",
            "metric.snapshot_date",
            "metric.instrument_family",
            "metric.metric_group",
            "metric.metric_category",
            "metric.metric_name"
        ]) }} AS fundamental_metric_pk,
        security.security_pk,
        metric.data_provider,
        metric.provider_exchange_code,
        metric.ticker,
        metric.instrument_family,
        metric.metric_group,
        metric.metric_category,
        metric.metric_name,
        metric.metric_value,
        metric.snapshot_date,
        metric.metric_date,
        metric.ingested_at
    FROM metric
    LEFT JOIN security
        ON metric.data_provider = security.data_provider
        AND metric.ticker = security.provider_symbol
)

SELECT * FROM final
