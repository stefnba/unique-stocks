# Exchange Timezones and UTC Offsets

Decision date: 2026-06-11

## Decision

Store exchange timezones as IANA timezone names from the provider and derive UTC
session timestamps plus UTC offsets for each local exchange trading date.

Do not store a static UTC offset on exchange metadata, call a timezone-offset API,
or maintain a dbt seed that maps timezone names to one offset.

## Rationale

The EODHD v2 exchange-details endpoint returns `Timezone` as an IANA timezone,
for example `America/New_York`, alongside local trading-session times. That
timezone is the durable exchange-calendar attribute.

A UTC offset is not durable exchange metadata. It is true only for a specific
local timestamp because daylight-saving and other timezone-rule changes can
shift the offset. For example, `America/New_York` is UTC-05:00 in January and
UTC-04:00 in July.

Using the IANA timezone name lets DuckDB/dbt derive the correct UTC instant for
each session date without adding provider calls, rate limits, or a manually
maintained transition calendar.

## Implementation

`gold.dim_exchange_trading_day` keeps the provider-supplied `timezone` and
local effective session times, then derives:

- `effective_session_open_local_at`
- `effective_session_close_local_at`
- `effective_session_open_utc_at`
- `effective_session_close_utc_at`
- `utc_offset_minutes_at_session_open`
- `utc_offset_minutes_at_session_close`

The effective close uses the early-close time when the holiday calendar marks a
trading date as an early close. Derived timestamps and offsets are null on
closed or unknown-calendar dates because there is no effective trading session.

## Override Policy

If a provider returns an invalid or incorrect IANA timezone, add a narrow
provider timezone override or validation seed that corrects the timezone name
for the affected provider schedule code. Do not add a static offset seed. Any
override should preserve an IANA timezone so downstream offset derivation remains
date-aware.
