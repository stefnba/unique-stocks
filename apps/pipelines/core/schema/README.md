# Schema DSL

`core.schema` describes lake table schemas without mixing table policy into
parser-owned Pydantic row models.

## Concepts

### Row Model

Row models are Pydantic models. They validate normalized rows produced by
domain parsers before those rows are written to Bronze.

```python
class ExchangeCatalogSnapshot(BronzeModel):
    snapshot_date: date
    provider_exchange_code: str
    name: str
    operating_mic_codes: str | None = None
```

### Table Model

Table models are plain Python classes. They describe physical lake storage and
reference a separate Pydantic `row_model`.

```python
class ExchangeCatalogTable(BronzeTableModel):
    table_name = "exchange_catalog"
    row_model = ExchangeCatalogSnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "data_provider")
    idempotency_columns = ("snapshot_date",)


EXCHANGE_CATALOG_TABLE = ExchangeCatalogTable

__all__ = ["ExchangeCatalogTable", "EXCHANGE_CATALOG_TABLE"]
```

`BronzeTableModel` adds the standard Bronze envelope columns for DDL:

- `ingestion_id`
- `data_provider`
- `raw_json`
- `row_hash`
- `source_uri`
- `ingested_at`

These fields are not part of parser-owned row models.

## DDL

Table models can render DuckDB/MotherDuck DDL:

```python
ExchangeCatalogTable.to_ddl()
```

The generated columns come from:

1. `row_model.model_fields`
2. Bronze envelope columns
3. `unique_columns`, rendered as a `UNIQUE (...)` constraint

## SQL Type Overrides

Most SQL types are inferred from Python annotations:

- `str` -> `VARCHAR`
- `int` -> `BIGINT`
- `float` -> `DOUBLE`
- `Decimal` -> `DECIMAL`
- `date` -> `DATE`
- `datetime` -> `TIMESTAMPTZ`
- mappings/sequences -> `JSON`

Use `Annotated` with `SqlColumn` when inference is not specific enough:

```python
Money = Annotated[Decimal, SqlColumn("DECIMAL(18, 6)")]

class PriceRow(BronzeModel):
    close: Money
```

## Validation

Concrete table classes fail fast at class definition time when:

- required metadata is missing
- `row_model` is not a Pydantic model class
- `unique_columns` or `idempotency_columns` is not a tuple of strings
- referenced columns do not exist in the physical table

`data_provider`, `raw_json`, `row_hash`, `source_uri`, and `ingested_at` are valid
Bronze table columns because `BronzeTableModel` adds them to the physical
schema.

## App Wiring

Domain-owned table specs live beside their row models:

```text
domains/exchange/models.py
domains/exchange/tables.py
```

Each `tables.py` exports both the table class and an uppercase constant pointing
to that class. Domain datasets and the app-level registry import the uppercase
constant, for example `EXCHANGE_CATALOG_TABLE`.

The app-level registry lives in `lake/schema.py`. It imports domain table specs
and exposes `ALL_TABLES`, which is used by `scripts/render_init_lake_sql.py`
to regenerate `scripts/init_lake.sql`.

## Current Tradeoff

Column metadata currently uses string tuples:

```python
unique_columns = ("snapshot_date", "provider_exchange_code", "data_provider")
```

Those strings are runtime-validated immediately, but Python type checkers do
not autocomplete or statically verify them from Pydantic model fields. A fully
typed column-ref API would require generated column classes, custom descriptors,
or a type-checker plugin. For now, this package keeps the implementation small
and relies on fail-fast validation plus tests.
