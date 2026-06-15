# ToDo

## Prefect Notifications

- Restore `PREFECT_NOTIFICATION_BLOCK_ID` support for managed Prefect automations. The current automation registry installs `DoNothing()` actions, so dbt failure, ingestion failure, stale-run, and cancellation alerts do not send notifications yet.
