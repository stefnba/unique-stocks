# S3 Landing Zone Setup

This runbook provisions the AWS resources used by the pipelines app to store raw provider payloads before parsing them into typed Bronze records.

For root account hardening, provisioner setup, IAM Identity Center, MFA, and general policy concepts, read [iam_guide.md](iam_guide.md) first. This guide assumes you already have a provisioner/admin identity available to the AWS CLI or SDK.

## Scope

This guide owns the app-specific landing-zone resources:

- A dedicated S3 bucket for raw ingestion payloads.
- Baseline bucket controls: public access blocked, ACLs disabled, default encryption, versioning, and HTTPS-only access.
- A dedicated least-privilege IAM user for the pipelines app.
- An inline IAM policy scoped to that one bucket.
- Optional access-key creation for environments that cannot use temporary AWS credentials.
- Prefect block saving so flows can load S3 access at runtime.

This guide does not create root, human admin, or provisioner identities. Those belong in [iam_guide.md](iam_guide.md).

## Project Wiring

The app loads S3 access through Prefect blocks:

- `config/aws_resources.py` defines the non-secret AWS resource defaults used by both provisioning and Prefect block wiring.
- `config/blocks.py` defines the `aws-credentials` and `s3-bucket` blocks.
- `DEFAULT_BUCKET_NAME` and `DEFAULT_REGION` in `config/aws_resources.py` are non-secret infrastructure values.
- Do not add bucket or region values to `.env`; the app does not read them from environment variables.
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are secrets and are read only when saving Prefect blocks.
- `make blocks-save` persists the current block definitions to the Prefect server.

After creating or rotating AWS credentials, set the secrets in local `.env` or in the production deployment platform, then run:

```bash
cd apps/pipelines
make blocks-save
```

For a fresh environment, `make setup` also runs `make blocks-save`.

## Prerequisites

- Project dependencies installed with `uv sync`.
- A provisioner identity from [iam_guide.md](iam_guide.md), or an equivalent AWS identity with permission to create S3 buckets, IAM users, IAM policies, and IAM access keys.
- AWS credentials available to boto3 through environment variables, shared AWS config, an SSO profile, or an instance/task role.
- The intended bucket name and region from `apps/pipelines/config/aws_resources.py`, unless you pass explicit overrides.

The AWS CLI is optional but useful for checking your active identity:

```bash
aws sts get-caller-identity --profile provisioner
```

If you use IAM Identity Center, log in before running the setup script:

```bash
aws sso login --profile provisioner
```

## Recommended Path

From `apps/pipelines/`, preview the provisioning plan first:

```bash
uv run python scripts/s3/setup_landing_zone.py --profile provisioner --dry-run
```

Create or update the bucket, bucket controls, IAM user, and inline policy:

```bash
uv run python scripts/s3/setup_landing_zone.py --profile provisioner
```

Access key creation is a separate, deliberate step. Run without `--create-access-key` first to provision the bucket and IAM user, verify the output, then re-run with the flag only when you are ready to immediately store the secret — AWS shows `SecretAccessKey` only once:

```bash
# Step 1 — provision everything except the access key (idempotent, safe to re-run)
uv run python scripts/s3/setup_landing_zone.py --profile provisioner

# Step 2 — create the access key only when ready to store it
uv run python scripts/s3/setup_landing_zone.py --profile provisioner --create-access-key
```

AWS shows the secret access key only once. Store the returned values in local `.env` or production secrets:

```bash
AWS_ACCESS_KEY_ID=<returned AccessKeyId>
AWS_SECRET_ACCESS_KEY=<returned SecretAccessKey>
```

Then save the Prefect blocks:

```bash
make blocks-save
```

## Script Details

Use `scripts/s3/setup_landing_zone.py` from `apps/pipelines/`. The script is idempotent for bucket creation, bucket settings, IAM user creation, and inline IAM policy updates.

By default, it imports the bucket and region from `config/aws_resources.py` and applies these controls:

- S3 Block Public Access with all four bucket-level settings enabled.
- S3 Object Ownership `BucketOwnerEnforced`, which disables ACLs.
- Default server-side encryption with SSE-S3.
- Bucket versioning.
- A bucket policy statement that denies non-TLS requests.
- Bucket-scoped IAM permissions for list, read, write, and multipart uploads.
- No `s3:DeleteObject` permission unless `--allow-delete` is passed.

The same script is available through Make:

```bash
make s3-landing-zone ARGS="--profile provisioner --dry-run"
```

Create a different bucket or IAM user:

```bash
uv run python scripts/s3/setup_landing_zone.py \
  --profile provisioner \
  --bucket unique-stocks-prod \
  --region eu-central-1 \
  --user unique-stocks-prod-pipelines
```

Grant object delete only if a real cleanup workflow needs it:

```bash
uv run python scripts/s3/setup_landing_zone.py --profile provisioner --allow-delete
```

The Python script reads the project defaults directly from `config/aws_resources.py`.

## Dedicated Bucket Assumption

Use a dedicated bucket for this app. The setup script preserves existing bucket-policy statements while upserting the `DenyNonTLS` statement, but a shared bucket still increases the chance of policy, lifecycle, and retention coupling between apps.

If AWS returns `AccessDenied` from `HeadBucket`, the bucket name may already exist in another account or be inaccessible to your provisioner. Choose a globally unique bucket name or verify ownership before continuing.

## Save Prefect Blocks

For local development:

```bash
cd apps/pipelines
cp .env.example .env
# edit .env and set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
make blocks-save
```

For production, set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the deployment platform, then run `make setup` once for a new environment or `make blocks-save` after rotating keys.

## Security Posture

This setup is appropriate for local development and small deployments that cannot use AWS temporary credentials. For production workloads running on AWS infrastructure, prefer an IAM role with temporary credentials over a long-lived IAM user access key. If the worker runs on a non-AWS VPS, static access keys may be the practical option; keep the dedicated IAM user least-privileged, store keys only in the deployment platform's secret store, and rotate or delete unused keys.

Consider adding these controls if the project grows or compliance requirements increase:

- Separate buckets or AWS accounts per environment.
- Account-level or organization-level S3 Block Public Access.
- CloudTrail monitoring for S3 and IAM activity.
- AWS Config or Security Hub checks for bucket exposure.
- SSE-KMS with a customer-managed key if key-level audit or tighter key control is required.
- Prefix-scoped IAM policies if multiple apps ever share a bucket.
- S3 lifecycle rules once retention expectations are clear.

## Rotation

AWS allows only two access keys per IAM user. To rotate credentials:

1. Create a second access key with `uv run python scripts/s3/setup_landing_zone.py --profile provisioner --create-access-key`.
2. Update local `.env` or production secrets.
3. Re-save Prefect blocks with `make blocks-save`.
4. Run a small pipeline check that writes to S3.
5. Disable the old key with `aws iam update-access-key --user-name <iam-user> --access-key-id <old-key-id> --status Inactive --profile provisioner`.
6. After the pipeline runs successfully, delete the old key with `aws iam delete-access-key --user-name <iam-user> --access-key-id <old-key-id> --profile provisioner`.

If the script reports that the IAM user already has two access keys, disable and delete an unused key before creating another one.

## Console Alternative

The same resources can be created manually in the AWS Console:

- Create a dedicated S3 bucket in the region from `config/aws_resources.py`.
- Block all public access.
- Enforce bucket ownership, then enable default encryption and versioning.
- Add a bucket policy statement that denies non-TLS requests.
- Create an IAM user for the pipeline with no console access.
- Attach an inline policy scoped to the bucket for list, read, write, and multipart uploads.
- Create an access key and store it only in `.env` or production secrets.
- Save Prefect blocks with `make blocks-save`.

Run the script with `--dry-run` to print the exact bucket-policy and IAM-policy JSON before recreating the setup manually. The scripted path is preferred because it is repeatable and easier to review.
