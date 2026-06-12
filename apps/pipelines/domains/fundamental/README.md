# Fundamental domain

Prefect flow `fundamental-quarterly` ingests EODHD fundamentals JSON per instrument into S3 landing, writes one document metadata row, and extracts a curated set of active `bronze.fundamental_*` slice tables.

The raw landed JSON is the replay source for the full provider document. Lower-priority sections such as split/dividend-count snapshots, ESG activity rows, and historical index components remain preserved there until a concrete mart or product need justifies modeling them.

## Ingestion batch date (`snapshot_date`)

Bronze idempotency is keyed by **`(snapshot_date, provider_exchange_code, provider_instrument_code)`**. That date is the **ingestion batch**, not the provider's economic "as of" field inside the JSON.

| Run style                 | How to set the batch                                                                                                                            |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Manual / daily**        | Omit dates; uses **today**                                                                                                                      |
| **Multi-day backfill**    | Use deployment `fundamental/backfill` with `continue_ingestion_batch: true`; reuses **`MAX(snapshot_date)`** from `bronze.fundamental_document` |
| **New backfill campaign** | Pass **`ingestion_batch_date`** or **`snapshot_date`** once at the start (e.g. `2026-06-02`) and keep using it                                  |

Without continuation, each calendar day opens a **new partition** and every instrument looks pending again even when you only hit a 429 quota limit yesterday.

### Parameters

- `ingestion_batch_date` - preferred explicit pin for a campaign
- `snapshot_date` - same partition key (alias)
- `continue_ingestion_batch` - when `true` and both dates are omitted, continue the latest bronze batch

Resolution logic lives in `domains/fundamental/batch.py`.

## Quota and resume

Use `max_provider_credits` on `fundamental-quarterly/backfill` to cap provider
spend before the EODHD daily quota. Each fundamentals call defaults to
`provider_credits_per_call = 10`, so a 100k-credit day should run with a
controlled `max_provider_credits` and `continue_ingestion_batch: true`.

If EODHD returns HTTP 429, the flow stops after the current fetch batch, records
the rate-limited instrument as failed, marks unscheduled instruments as skipped/deferred
run units, writes `pipeline.ingestion_coverage` rows with
`status = 'provider_quota_deferred'`, and leaves them pending for the next run
under the same batch date. Credit-budget deferrals use the same audit status
with `reason = 'credit_budget_exhausted'`.

`provider_quota_deferred` is not a completion marker. It records why an instrument was
not submitted in this run; Bronze idempotency still controls whether a future
run treats that instrument snapshot as done.

## Deployments

See `prefect.yaml`: `fundamental-quarterly/manual`, `/backfill`, `/replay`.
