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

| Folder           | Owns                                                                                                                                  | Does not own                                                                       |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `config/`        | Environment-backed settings, stable enums, identity keys, and static non-secret defaults.                                             | Prefect block construction, registries, factories, runtime behavior.               |
| `control_plane/` | App-specific Prefect blocks, Prefect setup composition, AWS resource naming, deployment/runtime wiring.                               | Generic Prefect helpers or generic S3 behavior.                                    |
| `orchestration/` | Thin Prefect flow entrypoints, post-ingestion dbt build policy, smoke-run composition, dbt/build mappings, cross-domain workflows.    | Domain ingestion internals or provider clients.                                    |
| `core/`          | Generic HTTP, storage, lake, ingestion, run tracking, schema/migration, Prefect helper, dbt subprocess, and utility primitives.       | App vocabulary, concrete providers, concrete domains, settings-backed composition. |
| `providers/`     | Concrete provider clients, provider API models, provider identifier rules, provider-owned normalization.                              | Domain table/write policy or app orchestration.                                    |
| `domains/`       | Domain models, Bronze table specs, datasets, parsers, tasks, request/result contracts, services, and domain-specific selection logic. | Runtime block definitions, dbt deployment names, or cross-domain workflow wiring.  |
| `lakehouse/`     | App-level lake schema registry and migration files.                                                                                   | Generic lake client, schema primitives, or domain row models.                      |
| `dbt/`           | Bronze-to-Silver-to-Gold transformations, seeds, macros, snapshots, and dbt tests.                                                    | Python provider fetching or Bronze writes.                                         |
| `dashboard/`     | Streamlit operational read surface and dashboard read models.                                                                         | Ingestion execution or transformation logic.                                       |
| `scripts/`       | Thin command-line adapters: args, environment defaults, console output, exit codes.                                                   | Reusable behavior.                                                                 |

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

- Domain catalog: `domains/registry.py`
- Provider catalog: `providers/registry.py`
- dbt asset and post-ingestion build mapping: `orchestration/domain_dbt.py`
- Prefect blocks and runtime service wiring: `control_plane/`
- Lake table registry: `lakehouse/schema.py`

`domains/registry.py` must stay a pure domain catalog. It should not contain dbt
asset group names, dbt selectors, deployment names, or runtime wiring.

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

`instrument` is the first migrated thin-flow domain:

```text
domains/instrument/contracts.py
domains/instrument/service.py
orchestration/flows/instrument.py
```

Other domains may still have temporary `domains/<domain>/flows.py` files. Move
them one at a time by extracting a request/result contract and a domain service
before moving the thin Prefect entrypoint to `orchestration/flows/`.

Recommended follow-up order:

1. `exchange`, because it is small but has multiple reference flows.
2. `exchange_schedule`, because it is medium-sized and exercises batch controls.
3. `eod_price`, after splitting daily and backfill services.
4. `fundamental`, after splitting selection, ingestion, writing, and summary
   helpers.

## Migration Checklist For A Domain

1. Add `domains/<domain>/contracts.py` with explicit request/result dataclasses.
2. Add `domains/<domain>/service.py` and move ingestion control out of the flow.
3. Keep provider fetches, landing writes, parsing, and Bronze writes as separate
   task/helper surfaces.
4. Add `orchestration/flows/<domain>.py` as the thin `@flow` entrypoint.
5. Update `prefect.yaml`, smoke composition, and tests to import the new flow.
6. Delete the old `domains/<domain>/flows.py`; do not add compatibility shims.
7. Run focused domain, orchestration, and deployment-entrypoint tests.
