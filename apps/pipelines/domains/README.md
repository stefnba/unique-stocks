# Domain packages

`domains/` contains app-owned ingestion logic. Keep reusable machinery in
`core/`, concrete provider API access in `providers/`, and app-level workflow
composition in `orchestration/`.

## Standard package shape

New ingestion domains should start with the smallest set of these modules and
add the others only when the behavior exists:

| Module         | Owns                                                                             |
| -------------- | -------------------------------------------------------------------------------- |
| `contracts.py` | Public request/result dataclasses for the domain service.                        |
| `service.py`   | The domain use case: selection, sequencing, counters, audit status, and summary. |
| `tasks.py`     | Prefect task wrappers around retryable IO such as provider calls and writes.     |
| `models.py`    | Domain Bronze row models and typed internal records.                             |
| `tables.py`    | Concrete Bronze table specs for the domain.                                      |
| `datasets.py`  | Landing and Bronze dataset definitions.                                          |
| `parsers.py`   | Provider/raw payload to typed Bronze parsing.                                    |
| `assets.py`    | Prefect asset materialization helpers.                                           |
| `sql/`         | File-backed SQL owned by the domain.                                             |

The preferred flow shape is:

```text
orchestration/flows/<domain>.py
  -> domains/<domain>/contracts.py
  -> domains/<domain>/service.py
  -> domains/<domain>/tasks.py
```

All active ingestion domains follow this shape today. Do not add
`domains/<domain>/flows.py`; Prefect entrypoints belong under
`orchestration/flows/`.

Some domains also own focused extension modules when the behavior would make the
standard modules too broad:

| Module pattern         | Use when                                                                |
| ---------------------- | ----------------------------------------------------------------------- |
| `universe.py`          | A domain needs reusable universe or selection rules.                    |
| `coverage.py`          | A domain needs durable coverage/idempotency helpers around core tables. |
| `batch.py`             | A domain needs resumable batch/date/quota planning.                     |
| `writers.py`           | A domain writes several closely related Bronze tables from one payload. |
| `provider_universe.py` | Provider-specific universe policy would crowd the domain service/tasks. |

`domains/exchange/tasks/` is an accepted exception to the single `tasks.py`
module because exchange reference ingestion has separate provider task groups.
Keep task sub-packages rare and provider- or workflow-specific.

## Placement rules

- Keep Prefect deployment names, dbt build names, and cross-domain workflow
  composition in `orchestration/`.
- Keep domain-specific post-ingestion orchestration that composes dbt, Prefect
  artifacts, or events in `orchestration/<domain>_post_ingestion.py`.
- Keep block declarations and runtime control-plane wiring in `control_plane/`.
- Keep provider-specific clients and raw provider response models in
  `providers/`.
- Do not import `orchestration` from domain services. A service should return a
  result; the thin flow decides whether to trigger dbt or compose another
  domain.
- Domain tasks may wrap external IO, but reusable storage, lake, HTTP, and run
  tracking primitives stay in `core/`.
