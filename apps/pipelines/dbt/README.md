# dbt

This dbt project transforms Python-written Bronze tables into Silver cleanup models and Gold marts.

## Conventions

This is the canonical source for dbt design, naming, structure, mart shape, lineage, materialization, documentation, and testing conventions. Other repository docs should link here instead of repeating these details.

### Sources

Source definitions live under `models/sources/`. dbt discovers source YAML anywhere under configured model paths, so the directory name and file name, such as `bronze.yml`, are project conventions rather than dbt-reserved names.

The dbt-visible names are the YAML `sources[].name` and `tables[].name` values. Staging models reference Bronze with calls such as `source('bronze', 'eod_price')`. Keep source YAML focused on external/Bronze relations and the Python-to-dbt handoff; downstream dbt models should use `ref()`.

### Model Layers

Use layer prefixes consistently:

| Layer        | Prefix           | Example                           | Purpose                                     |
| ------------ | ---------------- | --------------------------------- | ------------------------------------------- |
| Staging      | `stg_`           | `stg_eod_price`                   | Cast, rename, normalize, and deduplicate.   |
| Intermediate | `int_`           | `int_exchange_universe`           | Reusable joins, classifications, and logic. |
| Gold mart    | `dim_` or `fct_` | `dim_exchange`, `fct_daily_price` | Consumer-facing dimensions and facts.       |

Use `dim_` for descriptive lookup, profile, and calendar tables. Use `fct_` for measurable events, observations, and metric facts. Keep business wording such as "exchange universe" in model descriptions rather than physical Gold table names.

Never put business logic in staging models. Put analytics logic in intermediate or mart models, depending on whether the logic is reusable structure or final consumer-facing business definition.

### Mart Layout

`dim_` and `fct_` models live only in `models/marts/`. Group mart files by useful business area/domain. Add a `core/` folder only when a conformed model is clearly shared across several areas; do not add structure before it earns its keep.

Current mart folders are grouped by business area/domain:

| Folder         | Current purpose                              |
| -------------- | -------------------------------------------- |
| `exchange/`    | Exchange dimensions and calendars.           |
| `security/`    | Security universe and profile dimensions.    |
| `price/`       | Price facts.                                 |
| `fundamental/` | Fundamental metric facts and related marts.  |

### SQL Shape

Mart SQL follows this pattern:

```sql
WITH upstream_model AS (
    SELECT *
    FROM {{ ref('int_or_stg_model') }}
),

final AS (
    SELECT
        {{ surrogate_key(["natural_key_column"]) }} AS example_pk,
        ...
    FROM upstream_model
)

SELECT * FROM final
```

Import CTEs come first, transformation CTEs come next, and the model ends with a `final` CTE plus `SELECT * FROM final`. Use `ref()` for upstream dbt models instead of hardcoded relation names.

### Keys And Columns

The first mart column is the deterministic primary key named after the model grain with a `_pk` suffix, such as `security_pk` or `daily_price_pk`. Use the local `surrogate_key()` macro for hashed keys. Avoid database identity or auto-increment keys in dbt models because they are not reproducible across rebuilds.

Columns are ordered primary key, foreign keys, text/categorical attributes, booleans, metrics/numerics, then dates/timestamps. Boolean columns use `is_` or `has_` prefixes. Date columns end in `_date`; timestamp columns end in `_at`.

Fact tables should include foreign keys to dimensions where the relationship is clear. Pull descriptive fields from staging/intermediate models, not from final dimensions, unless the mart only needs the dimension's key.

### Lineage Fields

Gold marts should expose fields consumers need directly. Keep raw lineage columns such as `source_uri` and `row_hash` upstream in Bronze/Silver unless a Gold consumer explicitly needs them. Snapshot dates and ingestion timestamps are fine in Gold when they clarify freshness.

### Materialization

Gold marts default to table materialization through `dbt_project.yml`. Use incremental materialization only for fact tables that are large or slow enough for full table rebuilds to become a real cost.

### Tests And Docs

Every model gets a `.yml` description file with model and column descriptions. Test files mirror model structure.

Every mart primary key gets `not_null` and `unique` schema tests. Fact-table foreign keys get `relationships` tests when the key is expected to map to a valid dimension row. Keep accepted-value tests on important enums and domain-specific sanity checks.

Keep standard key tests in schema YAML. Use singular SQL tests for custom assertions that generic dbt tests cannot express cleanly.

### Renames

When renaming a Gold mart, remember that dbt will create the newly named relation but will not automatically drop the old one from an existing lake. Drop deprecated Gold relations only after the replacement model has built and downstream consumers have moved.
