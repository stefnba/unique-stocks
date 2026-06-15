# Pipeline Architecture

This document is the target architecture for `apps/pipelines`. It explains
where code belongs, which direction dependencies should point, and how domain
ingestion should be structured as the app grows.

## Layer Rules

Pipeline code should follow this dependency direction:

```text
control_plane -> config
orchestration -> domains -> providers -> core
orchestration -> dbt/lakehouse/control_plane when composing app workflows
lakehouse -> domains + core audit table specs
scripts -> one app/core function, then exit code/output
```

`core/` is the reusable foundation. It must not import concrete providers,
concrete domains, app registries, app settings, Prefect block names, dbt asset
groups, or business-specific table manifests.

## Folder Placement

| Folder           | Owns                                                                                                                                                  | Does not own                                                                       |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `config/`        | Environment-backed settings, stable enums, identity keys, and static non-secret defaults.                                                             | Prefect block construction, registries, factories, runtime behavior.               |
| `control_plane/` | App-specific Prefect blocks, limit declarations, automation definitions, deployment defaults, AWS resource naming, deployment/runtime wiring.         | Generic Prefect helpers or generic S3 behavior.                                    |
| `orchestration/` | Thin Prefect flow entrypoints, post-ingestion dbt build policy, flow-check composition, dbt/build mappings, cross-domain workflows.                   | Domain ingestion internals or provider clients.                                    |
| `core/`          | Generic HTTP, storage, lake, ingestion, run tracking, schema/migration, infrastructure/orchestration helpers, dbt subprocess, and utility primitives. | App vocabulary, concrete providers, concrete domains, settings-backed composition. |
| `providers/`     | Concrete provider clients, provider API models, provider identifier rules, provider-owned normalization.                                              | Domain table/write policy or app orchestration.                                    |
| `domains/`       | Domain models, Bronze table specs, datasets, parsers, tasks, request/result contracts, services, and domain-specific selection logic.                 | Runtime block definitions, dbt deployment names, or cross-domain workflow wiring.  |
| `lakehouse/`     | App-level lake schema registry and migration files.                                                                                                   | Generic lake client, schema primitives, or domain row models.                      |
| `dbt/`           | Bronze-to-Silver-to-Gold transformations, seeds, macros, snapshots, and dbt tests.                                                                    | Python provider fetching or Bronze writes.                                         |
| `dashboard/`     | Streamlit operational read surface and dashboard read models.                                                                                         | Ingestion execution or transformation logic.                                       |
| `scripts/`       | Thin command-line adapters: args, environment defaults, console output, exit codes.                                                                   | Reusable behavior.                                                                 |

## Naming Decisions

- The app-specific runtime wiring package is `control_plane/`, not
  `platform/`, because a top-level Python package named `platform` can shadow
  Python's standard-library `platform` module.
- The app-level lake registry is `lakehouse/`; generic lake clients and schema
  primitives stay in `core/lake/`.
- `config/` is passive. If a module constructs a block, registers a service, or
  wires runtime systems together, it does not belong in `config/`.

## Registry Ownership

Keep registries close to the thing they register:

- Provider catalog: `providers/registry.py`
- dbt asset and post-ingestion build mapping: `orchestration/domain_dbt.py`
- Generic runtime infrastructure mechanics such as Prefect block handles and health checks: `core/infrastructure/`
- Orchestration event vocabulary, event publishing, asset materialization helpers, deployment sync mechanics, automation sync, and global-limit sync: `core/orchestration/`
- App Prefect blocks, limits, automations, and deployment defaults: `control_plane/prefect/`
- Lake table registry: `lakehouse/schema.py`

Do not add a domain registry until production code consumes one. Domain identity
keys live in `config.domains`; concrete domain behavior stays in each
`domains/<domain>/` package; dbt/build metadata lives in
`orchestration/domain_dbt.py`.

## Thin Flow Pattern

Prefect flow entrypoints should be thin wrappers. They normalize public
parameters, build a domain request, call a domain service, apply app-level
post-ingestion orchestration, and return the summary.

```text
orchestration/flows/<domain>.py
  @flow public entrypoint
  -> domains/<domain>/contracts.py
  -> domains/<domain>/service.py
  -> orchestration/post_ingestion.py when requested
```

Domain services own ingestion control:

- date/default resolution
- domain selection and idempotency checks
- provider fetch batching/concurrency
- landing writes
- parsing and Bronze writes
- rejection records
- durable run tracking
- materialization events
- summary/counter construction

In other words, `service.py` is the domain use case. It answers: "What does
this domain refresh mean end to end?" Put sequencing, decisions, counters, audit
status, and the returned summary shape there.

Domain tasks own Prefect task wrappers around concrete IO steps. They answer:
"What concrete IO step should Prefect run, retry, name, and observe?" Put
provider calls, S3 writes, lake writes, and existence checks in `tasks.py`.

Domain parsers, datasets, and tables stay separate so they remain testable
without a Prefect flow context.

## Current Migration Status

The active ingestion domains now use the thin-flow pattern:

```text
domains/exchange/contracts.py
domains/exchange/service.py
orchestration/flows/exchange.py

domains/exchange_schedule/contracts.py
domains/exchange_schedule/service.py
orchestration/flows/exchange_schedule.py

domains/instrument/contracts.py
domains/instrument/service.py
orchestration/flows/instrument.py

domains/eod_price/contracts.py
domains/eod_price/service.py
orchestration/flows/eod_price.py

domains/fundamental/contracts.py
domains/fundamental/service.py
domains/fundamental/writers.py
orchestration/flows/fundamental.py
```

New domains should start in this shape. Do not add new
`domains/<domain>/flows.py` files; Prefect entrypoints belong under
`orchestration/flows/`.

## Migration Checklist For A Domain

1. Add `domains/<domain>/contracts.py` with explicit request/result dataclasses.
2. Add `domains/<domain>/service.py` and move ingestion control out of the flow.
3. Keep provider fetches, landing writes, parsing, and Bronze writes as separate
   task/helper surfaces.
4. Add `orchestration/flows/<domain>.py` as the thin `@flow` entrypoint.
5. Update `prefect.yaml`, flow-check composition, and tests to import the new flow.
6. Delete any old `domains/<domain>/flows.py`; do not add compatibility shims.
7. Run focused domain, orchestration, and deployment-entrypoint tests.
