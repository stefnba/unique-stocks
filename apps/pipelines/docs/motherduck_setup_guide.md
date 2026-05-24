# MotherDuck Setup

This runbook sets up MotherDuck as the production lake backend for the pipelines app. Local development can keep using the DuckDB file from `LOCAL_LAKE_PATH`; set `MOTHERDUCK_TOKEN` only when you intentionally want to target MotherDuck.

## Scope

This guide owns:

- Creating or joining the MotherDuck organization.
- Creating the `unique_stocks` database.
- Creating and storing the token used by the pipelines app and dbt.
- Running the project setup commands against MotherDuck.
- Basic security and rotation practices.

This guide does not replace the local DuckDB workflow. Keep the local file path for fast development and use MotherDuck for shared or production runs.

## Organization Setup

Create the organization in the [MotherDuck UI](https://app.motherduck.com/) during signup, or ask an existing organization admin to invite you.

Recommended setup:

- Use one organization for the project team, not separate personal organizations for production data.
- Choose the cloud region carefully during organization creation. MotherDuck organizations are region-scoped.
- Keep at least two admins where possible, so access is not tied to one person.
- Invite teammates from **Settings -> Members** and use the `Member` role unless someone needs organization administration.
- Review members when someone leaves the team. Removing a user from an organization is destructive in MotherDuck, so double-check before doing it.

## Token Strategy

For local development, a personal read/write token is acceptable. For production, prefer a service account token so automation is not tied to a human account.

Token handling rules:

- Store tokens only in `.env` files, deployment platform secrets, or a secret manager.
- Never commit tokens or paste them into docs, issue comments, logs, or shell history.
- Use an expiration date for personal tokens when practical.
- Name tokens by purpose, for example `unique-stocks-local-stefan` or `unique-stocks-prod-pipelines`.
- Rotate tokens after exposure, role changes, or deployment ownership changes.

To create a personal token:

1. Open the [MotherDuck UI](https://app.motherduck.com/).
2. Click the organization name in the top-left, then **Settings**.
3. Click **Create token**.
4. Choose a recognizable name.
5. Choose `Read/Write` for ingestion and dbt builds.
6. Set an expiration if appropriate.
7. Copy the token immediately and store it securely.

For production service accounts, use **Settings -> Service Accounts** in the MotherDuck UI. Create a service account, create a read/write token for it, then store that token as `MOTHERDUCK_TOKEN` in the deployment platform.

## Project Environment

From `apps/pipelines/`, create or edit local `.env`:

```bash
cp .env.example .env
```

Set:

```bash
MOTHERDUCK_TOKEN=<token>
DBT_TARGET=prod
```

`MOTHERDUCK_TOKEN` is the project environment variable. MotherDuck's DuckDB connection parameter is named `motherduck_token`; the repo passes the project variable into that parameter where needed.

Keep `DBT_TARGET=dev` when you want dbt to use the local DuckDB file. The committed dbt profile already maps:

- `dev` -> local DuckDB via `DBT_DUCKDB_PATH`
- `prod` -> `md:unique_stocks` using `MOTHERDUCK_TOKEN`

## CLI Setup

Install DuckDB if you want direct CLI access:

```bash
brew install duckdb
```

Connect interactively:

```bash
duckdb "md:unique_stocks"
```

If this is your first CLI connection, DuckDB will load the MotherDuck extension and prompt you through browser authentication. For non-interactive project commands, use the project `MOTHERDUCK_TOKEN` environment variable instead of browser auth.

If you are using the DuckDB CLI outside the project Make targets and want token auth instead of browser auth, export MotherDuck's lower-case token variable from your existing project secret:

```bash
export motherduck_token="$MOTHERDUCK_TOKEN"
duckdb "md:unique_stocks"
```

Create the project database if it does not exist:

```sql
CREATE DATABASE IF NOT EXISTS unique_stocks;
USE unique_stocks;
SHOW DATABASES;
```

You can also run the project lake initializer directly:

```bash
cd apps/pipelines
make lake-init
```

The initializer runs `scripts/init_lake.sql` and is idempotent.

## dbt Setup

Validate the dbt connection:

```bash
cd apps/pipelines
DBT_TARGET=prod make dbt-debug
```

Run the full build:

```bash
DBT_TARGET=prod make dbt-build
```

For production setup, set `MOTHERDUCK_TOKEN` in the deployment platform first, then run:

```bash
PREFECT_API_URL=https://prefect.yourdomain.com/api \
ENVIRONMENT=prod \
DBT_TARGET=prod \
make setup
```

## What Can Be Done From CLI?

Yes for query and project setup:

- Connect with the DuckDB CLI using `duckdb "md:unique_stocks"`.
- Create databases and schemas with SQL.
- Run `make lake-init`, `make setup`, and dbt commands with `MOTHERDUCK_TOKEN` set.

Mostly no for organization administration:

- Organization creation, member invites, role changes, and personal token creation are UI-first.
- Service accounts and service-account tokens can be managed in the UI.
- MotherDuck also exposes an Admin REST API for service-account automation, authenticated with an admin token, but those endpoints are administrative and should be used deliberately.

## Security Checklist

- Use a service account token for production automation.
- Keep production tokens out of local shell history. Prefer `.env`, Coolify secrets, or another secret store.
- Do not put the token directly in a command-line connection string unless you are in a throwaway shell, because command history and process listings can expose it.
- Use personal tokens only for personal development.
- Give organization admin rights only to people who need them.
- Rotate tokens by creating a new token, updating `.env` or deployment secrets, running `make lake-init` or `make dbt-debug`, then revoking the old token.
- If a token leaks, revoke it immediately in the MotherDuck UI or via the Admin REST API if the token belongs to a service account.

## Troubleshooting

If `make lake-init` or `make dbt-debug` cannot authenticate:

- Confirm `MOTHERDUCK_TOKEN` is set in the same shell running the command.
- Confirm the token is `Read/Write`, not read-scaling/read-only.
- Confirm you are targeting `DBT_TARGET=prod` for dbt.
- Try an interactive check with `duckdb "md:unique_stocks"` to separate DuckDB/MotherDuck auth from project wiring.
- If the database is missing, create it in the MotherDuck UI or DuckDB CLI, then rerun `make lake-init`.

## References

- [MotherDuck: Authenticating to MotherDuck](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/)
- [MotherDuck: Connecting to MotherDuck](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/connecting-to-motherduck/)
- [MotherDuck: Using the DuckDB CLI](https://motherduck.com/docs/getting-started/connect-query-from-duckdb-cli)
- [MotherDuck: Managing Organizations](https://motherduck.com/docs/key-tasks/managing-organizations/)
- [MotherDuck: Managing Service Accounts](https://motherduck.com/docs/key-tasks/service-accounts-guide/)
