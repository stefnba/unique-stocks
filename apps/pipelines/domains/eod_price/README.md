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
pending = latest bronze.instrument tickers for the exchange
        - tickers with any bronze.eod_price row for that exchange
        - tickers with pipeline.ingestion_coverage (no_data, exact ticker_backfill date range)
```

Use `max_provider_calls` on `eod-price-backfill/historical-backfill` to stop
before the provider's daily call quota. For example, if the provider account has
100k daily calls and the exchange universe is 150k symbols, run with a cap below
100k, then re-run the next day with the same `from_date`/`to_date`. The next run
recomputes pending symbols from Bronze plus exact-range coverage and continues
with the remaining tickers.

If the provider returns HTTP 429, the flow stops scheduling later batches. The
429 ticker is recorded as a failed run unit, unscheduled tickers stay pending,
and no `no_data` coverage row is written for quota failures.

### `pipeline.ingestion_coverage` (EOD conventions)

Cross-domain pipeline table; EOD backfill uses:

| Field           | EOD value                                                        |
| --------------- | ---------------------------------------------------------------- |
| `domain`        | `eod_price`                                                      |
| `unit_type`     | `ticker_backfill`                                                |
| `unit_key_json` | `{provider_exchange_code, ticker, from_date, to_date}`           |
| `status`        | `no_data` when fetch+landing succeeded but the provider returned no rows |
| `reason`        | `no_valid_rows`                                                  |

Helpers: `domains/eod_price/coverage.py` (unit key builder), `core/ingestion/coverage.py` (generic read/write). Coverage lookup pushes `unit_key_json` field matches into SQL, then applies a Python fallback filter after decoding DuckDB JSON strings.

**Not written** for fetch failures, HTTP 429, or all-rows-rejected parser outcomes - those symbols stay pending (Bronze idempotency handles completed symbols).

**Future:** other domains can use the same table (for example `fundamental` + `ticker_snapshot` + `provider_quota_deferred` for tickers skipped after quota exhaustion without a bronze row).

**Force retry:** delete the matching `ingestion_coverage` row (and any `bronze.eod_price` rows if re-ingesting prices), then re-run backfill.

```sql
SELECT unit_key_json, run_id, rows_raw, rows_valid, source_uri
FROM pipeline.ingestion_coverage
WHERE domain = 'eod_price'
  AND unit_type = 'ticker_backfill'
  AND status = 'no_data'
  AND json_extract_string(unit_key_json, '$."provider_exchange_code"') = 'US'
  AND json_extract_string(unit_key_json, '$."from_date"') = '2026-05-01'
  AND json_extract_string(unit_key_json, '$."to_date"') = '2026-05-31';
```

See [`docs/pipeline_audit.md`](../../docs/pipeline_audit.md) and [`core/ingestion/coverage.py`](../../core/ingestion/coverage.py).

## Daily bulk flow

`eod-price-daily` does not write coverage rows. Idempotency is per `(provider_exchange_code, bar_date)` on `bronze.eod_price`.

## Local smoke

```bash
uv run python scripts/run_smoke.py eod-price
```

Per-ticker backfill is started via Prefect (`eod-price-backfill/historical-backfill`), not the smoke preset.
