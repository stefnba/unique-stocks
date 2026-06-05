# EOD price domain

Prefect flows and tasks for end-of-day OHLCV ingestion from EODHD.

## Flows

| Flow                 | Endpoint grain                               | Lake output                                       |
| -------------------- | -------------------------------------------- | ------------------------------------------------- |
| `eod-price-daily`    | One bulk call per exchange per trade date    | `bronze.eod_price`                                |
| `eod-price-backfill` | One call per qualified ticker and date range | `bronze.eod_price`, `pipeline.ingestion_coverage` |

Deployments are defined in `prefect.yaml` (`eod-price-daily`, `eod-price-backfill`).

## Historical backfill resume

Backfill pending symbols are computed in `load_backfill_pending_symbols`:

```text
pending = silver.int_eod_price_backfill_symbol_status provider_symbol values for the exchange
        - tickers whose min/max completed bars span the requested date range
        - tickers in silver.int_eod_price_backfill_no_data_coverage for the exact date range
```

Use `max_provider_calls` on `eod-price-backfill/historical-backfill` to stop
before the provider's daily call quota. For example, if the provider account has
100k daily calls and the exchange universe is 150k symbols, run with a cap below
100k, then re-run the next day with the same `from_date`/`to_date`. The next run
recomputes pending symbols from the Silver backfill symbol-status view and
Silver exact-range no-data coverage, then continues with the remaining tickers.

If the provider returns HTTP 429, the flow stops scheduling later batches. The
429 ticker is recorded as a failed run unit, unscheduled tickers get
`provider_quota_deferred` audit coverage, and no `no_data` coverage row is
written for quota failures. Deferred tickers stay pending for the next run.

### `pipeline.ingestion_coverage` (EOD conventions)

Cross-domain pipeline table; EOD backfill uses:

| Field           | EOD value                                                        |
| --------------- | ---------------------------------------------------------------- |
| `domain`        | `eod_price`                                                      |
| `unit_type`     | `ticker_backfill`                                                |
| `unit_key_json` | `{provider_exchange_code, ticker, from_date, to_date}`           |
| `status`        | `no_data` after fetch+landing returned no rows; `provider_quota_deferred` for unsubmitted quota-deferred work |
| `reason`        | `no_valid_rows`                                                  |

Helpers: `domains/eod_price/coverage.py` (unit key builder), `core/ingestion/coverage.py` (generic write/idempotency). dbt exposes EOD no-data rows through `silver.int_eod_price_backfill_no_data_coverage`; Python pending selection does not parse coverage JSON directly.

Only `no_data` coverage removes a ticker from pending-symbol planning. `provider_quota_deferred` is an audit row for
unsubmitted work and deliberately leaves the ticker pending.

**Not written** as `no_data` for fetch failures, HTTP 429, or all-rows-rejected parser outcomes - those symbols stay pending (Bronze idempotency handles completed symbols).

Fundamentals also uses the same table with `domain = 'fundamental'`, `unit_type = 'ticker_snapshot'`, and
`status = 'provider_quota_deferred'` for ticker snapshots skipped after credit or rate-limit exhaustion.

**Force retry:** delete the matching `ingestion_coverage` row (and any `bronze.eod_price` rows if re-ingesting prices), rebuild `dbt-build/price-build`, then re-run backfill.

```sql
SELECT provider_symbol, unit_key_hash, rows_raw, rows_valid, source_uri
FROM silver.int_eod_price_backfill_no_data_coverage
WHERE provider_exchange_code = 'US'
  AND from_date = DATE '2026-05-01'
  AND to_date = DATE '2026-05-31';
```

See [`docs/pipeline_audit.md`](../../docs/pipeline_audit.md) and [`core/ingestion/coverage.py`](../../core/ingestion/coverage.py).

## Daily bulk flow

`eod-price-daily` does not write coverage rows. Idempotency is per `(provider_exchange_code, bar_date)` on `bronze.eod_price`.
When `trade_date` is explicit and Bronze already has that exchange/date, the flow
skips the provider call and records the exchange/date run unit as
`status = 'skipped'`, `reason = 'already_ingested'`. Provider-latest runs with
no `trade_date` still fetch first because the bar date is unknown before the
provider response.

## Local smoke

```bash
uv run python scripts/run_smoke.py eod-price
```

Per-ticker backfill is started via Prefect (`eod-price-backfill/historical-backfill`), not the smoke preset.
