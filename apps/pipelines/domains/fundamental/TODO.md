# Fundamentals TODO

## Current Direction

- [x] Preserve the full provider fundamentals document in landing storage.
- [x] Write document metadata to `bronze.fundamental_document`.
- [x] Split stock identity into `bronze.fundamental_stock_identity`.
- [x] Flatten stock financial statements into `bronze.fundamental_statement_fact`.
- [x] Complete fixture-backed stock fundamentals Bronze coverage.
- [x] Add dbt staging models for each Bronze fundamentals table.
- [ ] Add async batching and provider-credit-aware rate limits for larger backfills.
- [x] Add ETF, fund, and index identity tables from the current fixtures.
- [x] Add ETF/fund holdings and index constituents as separate edge-table grains.
- [ ] Add replay from landed fundamentals JSON without spending provider credits.

## Stock Bronze Coverage

- [x] Earnings estimates, history, and annual actuals.
- [x] Shares stats and outstanding shares.
- [x] Holders.
- [x] Splits and dividends.
- [x] Compact numeric metrics from Highlights, Valuation, Technicals, AnalystRatings, and ESGScores.
- [x] ESG activity involvement rows.
- [ ] Insider transactions, once a non-empty real fixture is available.

## Notes

- Keep the provider document as the durable source of truth.
- Prefer long-form fact tables for nested metric maps that can grow provider fields.
- Add class-specific identity and edge tables only when the instrument family has real fixtures.
- Treat `snapshot_date` as the ingestion batch date; use `payload_hash` refreshes for same-day provider changes.

## Multi-Family Coverage

- [x] ETF identity from `ETF_Data`.
- [x] ETF holdings from `ETF_Data.Holdings` / `ETF_Data.Top_10_Holdings`.
- [x] ETF allocation/region/sector/performance metric facts.
- [x] Mutual fund identity from `MutualFund_Data`.
- [x] Mutual fund top holdings.
- [x] Mutual fund allocation/region/sector/performance metric facts.
- [x] Index identity from index fixtures.
- [x] Index constituents / components from index fixtures.

## Production Hardening

- [x] dbt staging for all Bronze fundamentals tables.
- [ ] Async batching with configurable batch size.
- [ ] Provider-credit-aware throttling.
- [ ] Replay from landed fundamentals JSON.
