# S3 Landing Zone Setup

This runbook creates the AWS S3 bucket and IAM user used by the pipelines app as an ingestion landing zone. It is a one-time setup per AWS account/environment unless you rotate keys or create a new bucket.

The setup can be done with the Python provisioning script. The AWS Console is useful for inspection, but it is not required for repeatable setup.

## Project wiring

The pipeline loads S3 access through Prefect blocks:

- `config/blocks.py` defines the `aws-credentials` and `s3-bucket` blocks.
- Bucket name and region are not secrets and are intentionally configured in `config/blocks.py`.
- Do not add bucket or region values to `.env`; the app does not read them from environment variables.
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are secrets and are read only when saving the Prefect blocks.
- `make blocks-save` persists the current block definitions to the Prefect server.

After creating or rotating AWS credentials, set the secrets in `.env` for local/dev or in the production deployment platform, then run:

```bash
cd apps/pipelines
make blocks-save
```

For a fresh environment, `make setup` also runs `make blocks-save`.

## Prerequisites

- Project dependencies installed with `uv sync`.
- AWS credentials available to boto3 as an administrator or provisioning role.
- Permission to create S3 buckets, IAM users, IAM policies, and IAM access keys.
- The bucket name and region from `apps/pipelines/config/blocks.py`.

The Python script uses the normal AWS credential provider chain: environment variables, shared AWS config files, SSO/profile credentials, or instance/task role credentials.

If you also want to use the temporary shell fallback, install AWS CLI v2 first.

Official macOS installer:

```bash
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "/tmp/AWSCLIV2.pkg"
sudo installer -pkg /tmp/AWSCLIV2.pkg -target /
aws --version
```

Homebrew alternative:

```bash
brew install awscli
aws --version
```

If the install succeeds but `zsh` still cannot find `aws`, open a new terminal and check that `/usr/local/bin` or `/opt/homebrew/bin` is on `PATH`.

Check your active AWS identity with the AWS CLI when available:

```bash
aws sts get-caller-identity
```

## Scripted setup

Use `scripts/setup_s3_landing_zone.py` from `apps/pipelines/`. The script is idempotent for bucket, bucket settings, IAM user, and IAM policy creation. It imports the default bucket and region from `config/blocks.py`, blocks public access, enforces bucket ownership, enables default encryption, and enables versioning. The IAM policy is scoped to one bucket and does not grant object deletion unless you pass `--allow-delete`. Access key creation is explicit because AWS allows only two active access keys per IAM user.

Preview the AWS commands first:

```bash
cd apps/pipelines
uv run python scripts/setup_s3_landing_zone.py --dry-run
```

The same script is available through Make:

```bash
make s3-landing-zone ARGS="--dry-run"
```

Create or update the default dev resources from `config/blocks.py`:

```bash
uv run python scripts/setup_s3_landing_zone.py
```

Create a different bucket or IAM user:

```bash
uv run python scripts/setup_s3_landing_zone.py \
  --bucket unique-stocks-prod \
  --region eu-central-1 \
  --user unique-stocks-prod-pipelines
```

Use a named AWS CLI profile:

```bash
uv run python scripts/setup_s3_landing_zone.py --profile personal
```

Create a new access key when you are ready to store credentials:

```bash
uv run python scripts/setup_s3_landing_zone.py --create-access-key
```

Grant object delete only if a real cleanup workflow needs it:

```bash
uv run python scripts/setup_s3_landing_zone.py --allow-delete
```

The older AWS CLI shell script is still available while the Python path settles:

```bash
scripts/setup_s3_landing_zone.sh --dry-run
```

AWS shows the secret access key only once. Store the returned values in local `.env` or your production secret store:

```bash
AWS_ACCESS_KEY_ID=<returned AccessKeyId>
AWS_SECRET_ACCESS_KEY=<returned SecretAccessKey>
```

Never commit these values.

## Save Prefect blocks

For local development:

```bash
cd apps/pipelines
cp .env.example .env
# edit .env and set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
make blocks-save
```

For production, set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the deployment platform, then run `make setup` once for a new environment or `make blocks-save` after rotating keys.

## Security posture

This setup is secure enough for local development and small deployments that cannot use AWS temporary credentials. For production workloads running on AWS infrastructure, prefer an IAM role with temporary credentials over a long-lived IAM user access key. If the worker runs on a non-AWS VPS, static access keys may be the practical option; keep the dedicated IAM user least-privileged, store keys only in the deployment platform's secret store, and rotate or delete unused keys.

The script intentionally applies these controls:

- S3 Block Public Access with all four bucket-level settings enabled.
- S3 Object Ownership `BucketOwnerEnforced`, which disables ACLs.
- Default server-side encryption with SSE-S3.
- Bucket versioning.
- Bucket-scoped IAM permissions for list, read, write, and multipart uploads.
- No `s3:DeleteObject` permission unless explicitly requested with `--allow-delete`.

Consider adding these controls if the project grows or compliance requirements increase:

- Separate buckets or AWS accounts per environment.
- Account-level or organization-level S3 Block Public Access.
- CloudTrail monitoring for S3 and IAM activity.
- AWS Config or Security Hub checks for bucket exposure.
- SSE-KMS with a customer-managed key if key-level audit or tighter key control is required.
- Prefix-scoped IAM policies if multiple apps ever share a bucket.

## Rotation

AWS allows two active access keys per IAM user. To rotate credentials:

1. Create a second access key with `aws iam create-access-key --user-name "$IAM_USER"`.
2. Update local `.env` or production secrets.
3. Re-save Prefect blocks with `make blocks-save`.
4. Disable the old key with `aws iam update-access-key --user-name "$IAM_USER" --access-key-id <old-key-id> --status Inactive`.
5. After the pipeline runs successfully, delete the old key with `aws iam delete-access-key --user-name "$IAM_USER" --access-key-id <old-key-id>`.

## Console alternative

The same resources can be created manually in the AWS Console:

- Create an S3 bucket in the region from `config/blocks.py`.
- Block all public access.
- Enforce bucket ownership, then enable default encryption and versioning.
- Create an IAM user for the pipeline.
- Attach a policy equivalent to the JSON above.
- Create an access key and store it only in `.env` or production secrets.

The CLI flow is preferred because it is repeatable and easy to review.
