# Pipeline Scripts

This folder contains command-line entrypoints for the pipelines app. Scripts are
grouped by operational area so `pyproject.toml`, Make targets, Docker
healthchecks, and local runbooks can point at stable, readable commands.

## Boundary

Scripts own command-line concerns:

- argument parsing and defaults
- environment-variable fallback behavior
- stdout/stderr messages
- process exit codes
- small operator-facing examples

Reusable behavior belongs in `core/`, `control_plane/`, `orchestration/`,
`domains/`, `providers/`, or `config/`.
If logic would be useful from a test, another script, a Prefect flow, or a
future service process, keep it outside `scripts/` and call it from the script.

As a rule of thumb: scripts translate "what the operator typed" into explicit
Python inputs, call one core function, then translate the result back into a
process outcome.

## Folders

- `health/`: Docker and production-facing healthcheck commands.
- `prefect/`: Prefect server setup and deployment-sync commands.
- `s3/`: S3 landing-zone provisioning command.
- `smoke/`: narrow local smoke-run command for live provider checks.
- `tooling/`: shell glue used by Make or the local developer toolchain.

Legacy flat script paths are intentionally unsupported. Prefer adding new
entrypoints to the relevant folder with a small test under `tests/unit/scripts/`.
Once an operator command is stable, expose it under `[project.scripts]` with a
`pipelines-*` name and have Make or Docker call that entry point instead of the
file path.
