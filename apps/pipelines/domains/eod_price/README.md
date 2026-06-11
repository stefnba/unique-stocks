# EOD price domain

Prefect flows and tasks for end-of-day OHLCV ingestion from EODHD.

## Flows

| Flow                 | Endpoint grain                            | Lake output                                       |
| -------------------- | ----------------------------------------- | ------------------------------------------------- |
| `eod-price-daily`    | One bulk call per exchange per trade date | `bronze.eod_price`                                |
| `eod-price-backfill` | One call per instrument and date window   | `bronze.eod_price`, `pipeline.ingestion_coverage` |

Deployments are defined in `prefect.yaml` (`eod-price-daily`, `eod-price-backfill`).

## Coverage invariant

The operational goal is:

```text
For every active tradable provider instrument on every enabled provider exchange,
every local exchange trading day is either priced, explicitly covered as no-data,
or flagged as an actionable gap.
```

The current flow mechanics cover pieces of that goal:

| Mechanism                   | Current behavior                                                                                                                                                                                                                                                         |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Daily upsert/idempotency    | Daily bulk skips explicit `(provider_exchange_code, bar_date)` partitions only when a `daily_bulk` Bronze row already exists and rebuilt exchange/day coverage has no blocking gap. Bronze uniqueness is per provider exchange, provider instrument, date, and provider. |
| Historical resume           | Backfill recomputes pending instruments from Silver instrument-day coverage plus exact-window terminal coverage rows. A canceled run resumes at the next not-covered instrument after dbt rebuilds the selector views.                                                   |
| Partial historical coverage | Explicit `from_date` backfills keep an instrument pending when any requested trading day is still `missing_price`. Open-start backfills require exact-window completed/no-data coverage because a single daily bar does not prove full history.                          |
| Row lineage                 | `bronze.eod_price.ingestion_mode` records whether the current stored bar came from `daily_bulk` or `historical_backfill`; this is not part of the Bronze unique key.                                                                                                     |
| Provider no-data            | A successful provider fetch plus landing write with zero rows records terminal `no_data` coverage for the exact instrument/window.                                                                                                                                       |
| Provider quota stop         | Submitted 429 failures and unsubmitted quota-deferred work are not marked no-data, so they remain retryable. Deferred coverage is audit metadata only.                                                                                                                   |
| Parser rejects              | Bad rows are recorded as rejections. Payloads with any rejected rows stay retryable for terminal coverage purposes rather than being marked exact-window completed/no-data.                                                                                              |
| Exchange calendars          | dbt maps EODHD provider exchange codes to schedule endpoint codes, then applies working days, holidays, and early closes to decide expected trading days. Unknown mappings are surfaced instead of silently treated as closed.                                           |

The new dbt control views make the invariant observable:

| Model                                          | Purpose                                                                                                                                                                                                                    |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `silver.int_exchange_trading_day`              | One row per enabled EOD provider exchange and calendar date, with known/unknown calendar, working-day, holiday, early-close, and trading-day flags.                                                                        |
| `silver.int_eod_price_expected_instrument_day` | Expected instrument/date rows for active tradable instruments on trading days, plus unknown-calendar dates. Observed price history, today's no-price instruments, and terminal no-data windows define the expected ranges. |
| `silver.int_eod_price_instrument_day_coverage` | Classifies each expected instrument/date as `priced`, `known_no_data`, `missing_price`, or `unknown_calendar`.                                                                                                             |
| `silver.int_eod_price_exchange_day_status`     | Exchange/date rollup for daily gates and monitoring: `complete`, `missing_price`, `unknown_calendar`, `closed_exchange`, or `no_expected_instruments`.                                                                     |

The dbt control views are also used operationally: post-ingestion price builds query `silver.int_eod_price_exchange_day_status`, and the EOD run is audited as `partial` when rebuilt coverage still contains `missing_price` or `unknown_calendar`.

Full historical assurance before an instrument's first observed price still needs a listing/delisting effective-date source, or a provider-backed terminal no-data window, before it can be enforced without false positives.

`silver.int_eod_price_instrument_history_bounds` exposes each provider exchange/instrument pair's first and latest observed EOD price dates. For open-start historical backfills, the provider determines the first available bar date; this is a provider-observed first price date for that exchange/instrument pair, not necessarily the official listing date. Exchange-level first trading dates should be treated the same way unless an authoritative exchange inception source is added.

## Historical backfill resume

Backfill pending instruments are computed in `load_backfill_pending_instruments`:

```text
pending = latest tradable provider instruments for the exchange
        - instruments whose requested instrument-days are fully priced
        - instruments in silver.int_eod_price_backfill_terminal_coverage for the exact requested window
```

Omit `from_date` on `eod-price-backfill/historical-backfill` to request each
instrument's full available EODHD history through `to_date` (default: today). In that
open-start mode, `from_date` is stored as JSON `null` in the backfill unit key
and the raw landing object uses `from_date=all`.

Use `max_provider_calls` on `eod-price-backfill/historical-backfill` to stop
before the provider's daily call quota. For example, if the provider account has
100k daily calls and the exchange universe is 150k instruments, run with a cap below
100k, then re-run the next day with the same `from_date`/`to_date` window. The next run
recomputes pending instruments from the Silver instrument-day coverage view and
Silver exact-window terminal coverage, then continues with the remaining instruments.

If the provider returns HTTP 429, the flow stops scheduling later batches. The
429 instrument is recorded as a failed run unit, unscheduled instruments get
`provider_quota_deferred` audit coverage, and no `no_data` coverage row is
written for quota failures. Deferred instruments stay pending for the next run.

### `pipeline.ingestion_coverage` (EOD conventions)

Cross-domain pipeline table; EOD backfill uses:

| Field           | EOD value                                                                                                                                                                            |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `domain`        | `eod_price`                                                                                                                                                                          |
| `unit_type`     | `instrument_backfill`                                                                                                                                                                |
| `unit_key_json` | `{provider_exchange_code, provider_instrument_code, from_date, to_date}`                                                                                                             |
| `status`        | `completed` after valid Bronze rows are written with no rejected rows; `no_data` after fetch+landing returned no rows; `provider_quota_deferred` for unsubmitted quota-deferred work |
| `reason`        | `price_rows_completed`, `no_valid_rows`, or the quota deferral reason                                                                                                                |

Helpers: `domains/eod_price/coverage.py` (unit key builder), `core/ingestion/coverage.py` (generic write/idempotency). dbt exposes EOD terminal rows through `silver.int_eod_price_backfill_terminal_coverage`; Python pending selection does not parse coverage JSON directly.

`completed` and `no_data` coverage remove an instrument from pending planning for the exact requested window. `provider_quota_deferred` is an audit row for unsubmitted work and deliberately leaves the instrument pending.

**Not written** as terminal coverage for fetch failures, HTTP 429, all-rows-rejected parser outcomes, or mixed valid/rejected parser outcomes - those instruments stay pending (Bronze idempotency handles valid inserted rows).

Fundamentals also uses the same table with `domain = 'fundamental'`, `unit_type = 'instrument_snapshot'`, and
`status = 'provider_quota_deferred'` for instrument snapshots skipped after credit or rate-limit exhaustion.

**Force retry:** delete the matching `ingestion_coverage` row (and any `bronze.eod_price` rows if re-ingesting prices), rebuild `dbt-build/price-build`, then re-run backfill.

```sql
SELECT provider_exchange_code, provider_instrument_code, unit_key_hash, rows_raw, rows_valid, source_uri
FROM silver.int_eod_price_backfill_terminal_coverage
WHERE provider_exchange_code = 'US'
  AND from_date IS NULL
  AND to_date = DATE '2026-05-31';
```

See [`docs/pipeline_audit.md`](../../docs/pipeline_audit.md) and [`core/ingestion/coverage.py`](../../core/ingestion/coverage.py).

## Daily bulk flow

`eod-price-daily` does not write terminal coverage rows. Idempotency is per
`(provider_exchange_code, bar_date, ingestion_mode = 'daily_bulk')`, and an
explicit `trade_date` skips only when the rebuilt exchange/day coverage view has
no blocking `missing_price` or `unknown_calendar` gap. If the previous daily run
was partial, rerunning the same explicit exchange/date fetches again and inserts
only still-missing Bronze keys. Provider-latest runs with no `trade_date` still
fetch first because the bar date is unknown before the provider response.
When `run_dbt_build=true`, provider-latest runs compare the returned provider
date with the latest expected exchange trading date from `silver.int_exchange_trading_day`;
a mismatch is audited as `partial`, and the coverage gate checks the expected date.

## Local smoke

```bash
uv run python scripts/run_smoke.py eod-price
```

Per-instrument backfill is started via Prefect (`eod-price-backfill/historical-backfill`), not the smoke preset.
