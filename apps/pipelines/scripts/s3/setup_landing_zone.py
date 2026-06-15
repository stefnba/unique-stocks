r"""CLI entrypoint for S3 landing-zone provisioning.

Run from ``apps/pipelines/``. All commands below assume that working directory.

Preview the provisioning plan without calling AWS::

    uv run python scripts/s3/setup_landing_zone.py --profile provisioner --dry-run

Create or update the bucket, security controls, IAM user, and inline policy::

    uv run python scripts/s3/setup_landing_zone.py --profile provisioner

Create an access key only when you are ready to store the secret immediately::

    uv run python scripts/s3/setup_landing_zone.py --profile provisioner --create-access-key

The same target is available through Make::

    make s3-landing-zone ARGS="--profile provisioner --dry-run"
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, ProfileNotFound

from control_plane.aws_resources import AWS_RESOURCES
from core.storage.s3.provisioning import (
    LandingZoneConfig,
    ProvisioningError,
    ProvisioningEvent,
    provision,
    validate_config,
)

DRY_RUN_LABELS = {
    "create_bucket_if_missing": "create bucket if missing",
    "put_public_access_block": "put public access block",
    "put_bucket_ownership_controls": "put bucket ownership controls",
    "put_bucket_encryption": "put bucket encryption",
    "put_bucket_versioning": "put bucket versioning",
    "upsert_tls_bucket_policy_statement": "upsert TLS-enforce bucket policy statement",
    "create_iam_user_if_missing": "create IAM user if missing",
    "put_inline_iam_user_policy": "put inline IAM user policy",
    "create_iam_access_key": "create IAM access key",
}


def build_parser() -> argparse.ArgumentParser:
    """Build the landing-zone provisioning CLI parser."""
    defaults = AWS_RESOURCES
    parser = argparse.ArgumentParser(
        description="Set up the S3 landing-zone bucket and IAM user for the pipelines app.",
    )
    parser.add_argument(
        "--bucket",
        default=defaults.bucket_name,
        help=f"S3 bucket name. Default: {defaults.bucket_name}",
    )
    parser.add_argument("--region", default=defaults.region, help=f"AWS region. Default: {defaults.region}")
    parser.add_argument("--user", default=defaults.iam_user, help=f"IAM user name. Default: {defaults.iam_user}")
    parser.add_argument(
        "--policy-name",
        default=defaults.inline_policy_name,
        help=f"Inline IAM policy name. Default: {defaults.inline_policy_name}",
    )
    parser.add_argument("--profile", default=None, help="AWS profile name to use.")
    parser.add_argument(
        "--create-access-key",
        action="store_true",
        help="Create and print a new access key for the IAM user.",
    )
    parser.add_argument(
        "--allow-delete",
        action="store_true",
        help="Grant s3:DeleteObject in the IAM policy. Off by default.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions without calling AWS.")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> LandingZoneConfig:
    """Parse command-line arguments into a provisioning config."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    config = LandingZoneConfig(
        bucket_name=args.bucket,
        region=args.region,
        iam_user=args.user,
        policy_name=args.policy_name,
        profile=args.profile,
        create_access_key=args.create_access_key,
        allow_delete=args.allow_delete,
        dry_run=args.dry_run,
    )
    validate_config(config)
    return config


def bool_label(value: bool) -> str:
    """Return a lowercase shell-style boolean label."""
    return "true" if value else "false"


def print_header(config: LandingZoneConfig) -> None:
    """Print resolved configuration before provisioning."""
    print("S3 landing-zone setup")
    print(f"  bucket:       {config.bucket_name}")
    print(f"  region:       {config.region}")
    print(f"  iam user:     {config.iam_user}")
    print(f"  policy name:  {config.policy_name}")
    print(f"  profile:      {config.profile or 'default'}")
    print(f"  allow delete: {bool_label(config.allow_delete)}")
    print(f"  dry run:      {bool_label(config.dry_run)}")


def _payload(event: ProvisioningEvent) -> Mapping[str, Any]:
    """Return an event payload mapping."""
    return event.payload or {}


def render_event(event: ProvisioningEvent) -> None:
    """Render one provisioning event for the CLI."""
    payload = _payload(event)
    if event.name == "dry_run":
        action = str(payload.get("action", "unknown_action"))
        print(f"DRY RUN: {DRY_RUN_LABELS.get(action, action)}")
        request = payload.get("request")
        if isinstance(request, Mapping) and request:
            print(json.dumps(request, indent=2, sort_keys=True))
        return
    if event.name == "bucket_exists":
        print(f"Bucket already exists: {payload.get('bucket_name', '')}")
        return
    if event.name == "bucket_created":
        print(f"Created bucket: {payload.get('bucket_name', '')}")
        return
    if event.name == "bucket_security_applied":
        print("Applied bucket security controls.")
        return
    if event.name == "tls_bucket_policy_applied":
        print("Applied TLS-enforce bucket policy statement.")
        return
    if event.name == "iam_user_exists":
        print(f"IAM user already exists: {payload.get('iam_user', '')}")
        return
    if event.name == "iam_user_created":
        print(f"Created IAM user: {payload.get('iam_user', '')}")
        return
    if event.name == "user_policy_attached":
        print(f"Attached inline IAM policy: {payload.get('policy_name', '')}")
        return
    if event.name == "access_key_skipped":
        iam_user = payload.get("iam_user", "")
        print(f"Access key creation skipped. Re-run with --create-access-key when ready for {iam_user}.")
        return
    if event.name == "access_key_created":
        print("Created access key. AWS shows SecretAccessKey only once; store it now.")
        print(json.dumps(payload, indent=2))
        return
    if event.name == "aws_caller_identity":
        print(f"AWS caller: {payload.get('arn', '')} (account {payload.get('account', '')})")
        return

    print(f"Provisioning event: {event.name}")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the S3 landing-zone provisioning CLI."""
    try:
        config = parse_args(argv)
        print_header(config)
        provision(config, emit=render_event)
        print("Done.")
        print("Next: put AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env or production secrets,")
        print("then run: make blocks-save")
    except (ProvisioningError, NoCredentialsError, ProfileNotFound) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "Unknown")
        message = error.get("Message", str(exc))
        print(f"AWS error ({code}): {message}", file=sys.stderr)
        return 1
    except BotoCoreError as exc:
        print(f"AWS client error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
