# ToDo

## Prefect Notifications

- [x] Wire managed Prefect automations to `BlockRegistry.SLACK_WEBHOOK`. Blank Slack configuration still installs explicit `DoNothing()` actions for local/dev, while configured environments install `SendNotification` actions.
