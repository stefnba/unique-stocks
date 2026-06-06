# AWS IAM Credentials & Access Management Guide

## What is IAM?

**Identity and Access Management** — AWS's system for controlling **who** can do **what** on **which resources**.

- **Identity** — users, groups, roles (the _who_)
- **Access** — policies that allow or deny actions (the _what_)
- **Management** — the tooling to create, assign, and audit all of it (the _which resources_, and the admin layer on top)

The root account exists **outside** of IAM and cannot be controlled by it — which is precisely why it's dangerous and why everything else should go through IAM.

---

## Why Not Root?

The AWS root user is created when you first open an AWS account. It has **unrestricted access to everything** — billing, account closure, IAM, all services — and critically, it **cannot be restricted by any IAM policy**.

This makes it dangerous for day-to-day use:

- A leaked root credential is a full account compromise
- No way to scope or limit what it can do
- AWS explicitly recommends against using it operationally
- Cannot be audited or restricted like regular IAM identities

**Root should only be used for:**

- Initial account bootstrap (one-time)
- Billing and account settings
- Recovering a fully locked-out account

---

## The Provisioner Pattern

The solution is a one-time bootstrap: use root **once** to create a provisioner identity, then lock root away forever.

```text
Root user  (one-time bootstrap only)
  └── Creates Provisioner user/identity  (broad but controlled admin access)
        ├── Creates project-a-s3-user  →  access only to bucket-a
        ├── Creates project-b-s3-user  →  access only to bucket-b
        └── ...
```

### Why a Provisioner?

|                                        | Root      | Provisioner |
| -------------------------------------- | --------- | ----------- |
| Can be restricted by IAM policies      | ✗         | ✓           |
| Supports MFA enforcement               | Limited   | ✓           |
| Can be audited via CloudTrail          | Partially | ✓           |
| Can be disabled/rotated if compromised | ✗         | ✓           |
| Safe for day-to-day use                | ✗         | ✓           |

### One Provisioner or Per Project?

**Single provisioner** is common for small teams — one admin identity used for all infrastructure work.

**Per-project service users** is the ideal end state: the provisioner creates a dedicated IAM identity per project/app with least-privilege permissions. This limits the blast radius if a credential is ever leaked.

---

## Console vs CLI — Two Separate Credential Systems

A common source of confusion: console login and CLI access use **completely different credentials**.

|             | Credential type             | Scope                    |
| ----------- | --------------------------- | ------------------------ |
| AWS Console | Username + password (+ MFA) | Browser session only     |
| CLI / SDK   | Access Key ID + Secret Key  | Programmatic access only |

A user can have one, both, or neither. They do not share credentials.

**CloudShell exception:** If you use AWS CloudShell (terminal inside the browser console), it automatically inherits the credentials of whoever is logged into the console — no separate CLI setup needed.

---

## Key Concepts

### What is a Policy?

A **policy** is a JSON document that defines what actions are allowed or denied, on which resources. You attach policies to users, groups, or roles to grant permissions.

```json
{
  "Effect": "Allow",
  "Action": "s3:*",
  "Resource": "arn:aws:s3:::my-app-bucket/*"
}
```

AWS ships hundreds of **managed policies** you can attach without writing JSON yourself:

| Policy                | What it allows                                   |
| --------------------- | ------------------------------------------------ |
| `AdministratorAccess` | Everything — equivalent to root for IAM purposes |
| `PowerUserAccess`     | Everything except IAM and account management     |
| `AmazonS3FullAccess`  | Full access to S3 only                           |
| `ReadOnlyAccess`      | Read-only access to all services                 |

When you run `attach-user-policy`, you're saying: _"give this user the permissions described in this policy."_

```bash
aws iam attach-user-policy \
  --user-name provisioner \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess
#                              ^^^^^^^ "aws" = AWS-managed, not yours
```

For project-specific users you'd write a custom **inline policy** — e.g. only allow read/write on one specific bucket — instead of attaching a broad managed policy.

---

### What is a Profile?

A **profile** is a named block of settings in your local AWS config files. It lets you store and switch between multiple identities/credentials.

Two files are involved:

```text
~/.aws/config       → region, SSO settings, output format
~/.aws/credentials  → access keys (Option B only)
```

A profile looks like:

```ini
# ~/.aws/config
[profile provisioner]
region = eu-central-1
sso_start_url = https://something.awsapps.com/start
```

You reference it with `--profile` or by setting an environment variable:

```bash
aws s3 ls --profile provisioner     # explicit, one command
export AWS_PROFILE=provisioner      # implicit, applies to all commands in session
```

The special profile named `default` is used automatically when no `--profile` is specified.

---

## Option A — IAM Identity Center (Proper SSO) ✅ Recommended

IAM Identity Center (formerly AWS SSO) is the modern, keyless approach to CLI authentication. It uses a browser-based flow to issue **short-lived temporary credentials** automatically.

### How It Works

```text
aws sso login --profile provisioner
  └── Opens browser → you authenticate
        └── Temporary credentials issued (~8hr TTL)
              └── CLI uses them automatically — no keys on disk
```

### Setup Steps

#### 1. Enable IAM Identity Center (console, as root — one last time)

Go to **AWS Console → IAM Identity Center**.

- If you see a blue **Enable** button → it's not yet enabled, click it
- AWS will offer to create an AWS Organization — accept it (required)
- Once enabled, you'll see a dashboard with a **SSO start URL** like `https://something.awsapps.com/start` — note this down
- To check if it's already enabled: the same page will show your dashboard instead of the Enable button; or run:

  ```bash
  aws sso-admin list-instances
  # Returns your Identity Center instance ARN and start URL if enabled
  # Returns an empty list if not enabled
  ```

#### 2. Create your provisioner user (console)

- Users → Add user (name + email)
- Permission sets → Create → choose `AdministratorAccess`
- AWS Accounts → Assign user → pick account + permission set
- Activate the user via the email invite

#### 3. Configure local CLI

```bash
aws configure sso --profile provisioner
# Enter: SSO start URL, SSO region
# Browser opens → authenticate → pick account + role
```

Your `~/.aws/config` will look like:

```ini
[profile provisioner]
sso_start_url = https://something.awsapps.com/start
sso_region = eu-central-1
sso_account_id = 123456789012
sso_role_name = AdministratorAccess
region = eu-central-1
```

#### 4. Daily usage

```bash
aws sso login --profile provisioner     # opens browser, ~1 min
aws sts get-caller-identity --profile provisioner  # verify
export AWS_PROFILE=provisioner          # avoid typing --profile every time
```

### Pros

- ✅ No long-lived credentials stored on disk
- ✅ Credentials auto-expire (~8 hours)
- ✅ Browser-based — familiar and phishing-resistant
- ✅ Works with AWS Toolkit in VS Code / Cursor
- ✅ Centrally managed — revoke access instantly from Identity Center
- ✅ Scales well across multiple accounts and team members
- ✅ Supports MFA natively

### Cons

- ❌ More upfront setup (requires enabling an AWS Organization)
- ❌ Requires re-login every ~8 hours
- ❌ Slightly more complex for automation/CI pipelines
- ❌ Tied to AWS Identity Center — another service to manage

---

## Option B — IAM User with Access Keys

The traditional approach: create a regular IAM user, generate static access keys, and store them in `~/.aws/credentials`.

### Setup Steps

#### 1. Create the user (as root, one-time)

```bash
aws iam create-user --user-name provisioner

aws iam attach-user-policy \
  --user-name provisioner \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess

aws iam create-access-key --user-name provisioner
# Save AccessKeyId and SecretAccessKey — shown only once
```

#### 2. Enable console login for the provisioner user

IAM users do not get console access by default — you must explicitly create a password for them:

```bash
aws iam create-login-profile \
  --user-name provisioner \
  --password 'ChangeMe123!' \
  --password-reset-required
```

Then log into the console at (not the root login page):

```text
https://<your-account-id>.signin.aws.amazon.com/console
```

#### 3. Enforce MFA for the provisioner user (console)

After logging in as provisioner:

1. Go to **IAM → Users → provisioner → Security credentials**
2. Click **Assign MFA device**
3. Choose Authenticator app (e.g. 1Password, Authy, Google Authenticator)
4. Scan the QR code and enter two consecutive OTP codes to confirm

To enforce MFA via policy (prevents any action if MFA not used), attach this inline policy to the user:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "NotAction": [
        "iam:CreateVirtualMFADevice",
        "iam:EnableMFADevice",
        "iam:GetUser",
        "iam:ListMFADevices",
        "sts:GetSessionToken"
      ],
      "Resource": "*",
      "Condition": { "BoolIfExists": { "aws:MultiFactorAuthPresent": "false" } }
    }
  ]
}
```

Important: this MFA-deny policy also affects CLI/API calls made with the user's long-lived access key, because those requests do not carry MFA context. Use it for a human provisioner only if you are prepared to obtain MFA-backed session credentials with `aws sts get-session-token`, or prefer IAM Identity Center for the provisioner path. Do not attach this kind of MFA enforcement policy to non-human app users such as `unique-stocks-pipelines`.

#### 4. Store credentials locally

```bash
aws configure --profile provisioner
# Enter: Access Key ID, Secret Access Key, region
```

This writes to `~/.aws/credentials`:

```ini
[provisioner]
aws_access_key_id = AKIA...
aws_secret_access_key = ...
region = eu-central-1
```

#### 5. Usage

```bash
aws s3 ls --profile provisioner
export AWS_PROFILE=provisioner  # set as default
```

### Pros

- ✅ Simple and quick to set up
- ✅ Works everywhere — any terminal, CI/CD, scripts
- ✅ No re-authentication required
- ✅ Well understood, widely documented

### Cons

- ❌ Long-lived credentials — don't expire unless manually rotated
- ❌ Keys stored in plaintext on disk (`~/.aws/credentials`)
- ❌ Easy to accidentally leak (committed to git, exposed in logs)
- ❌ Rotation is manual — easy to neglect
- ❌ Harder to revoke quickly if compromised
- ❌ Does not work with browser-based auth flow

---

## Comparison Summary

|                     | SSO (Identity Center)  | Access Keys             |
| ------------------- | ---------------------- | ----------------------- |
| Credentials on disk | ✗ None                 | ✓ Plaintext keys        |
| Credential lifetime | ~8 hours (auto-expire) | Permanent until rotated |
| Setup complexity    | Medium                 | Low                     |
| Re-authentication   | Every ~8 hours         | Never                   |
| CI/CD pipelines     | Needs workaround       | Works natively          |
| Team scalability    | Excellent              | Gets messy              |
| If compromised      | Auto-expires soon      | Must manually revoke    |
| AWS recommendation  | ✅ Preferred           | Use with caution        |

---

## Best Practices Checklist

- [ ] Never use root for day-to-day operations
- [ ] Enable MFA on the root account immediately
- [ ] Create a provisioner/admin identity as the first step
- [ ] Use IAM Identity Center (SSO) over static access keys where possible
- [ ] Apply least-privilege: per-project service users with only the permissions they need
- [ ] Never commit credentials to version control (use `.gitignore` on `~/.aws`)
- [ ] If using access keys, rotate them regularly (every 90 days)
- [ ] Enable CloudTrail to audit all API calls
- [ ] Set a billing alert so unexpected activity is caught early

---

## Project Users — Least-Privilege Access per App

### Rationale

The provisioner user is your admin identity — it has broad permissions and should only be used for infrastructure work. Any application (e.g. a backend that reads/writes S3) should get its own dedicated IAM user with **only the permissions it actually needs**.

This is called the **principle of least privilege**, and it matters because:

- If an app's credentials leak, the damage is contained to that app's resources only
- You can revoke or rotate one app's credentials without affecting anything else
- It's clear from IAM who has access to what
- Auditing is easier — each user's activity in CloudTrail maps to one app

```text
provisioner
  ├── my-app-s3-user   →  read/write my-app-bucket only
  ├── analytics-user   →  read-only on analytics-bucket only
  └── backup-user      →  write-only on backup-bucket only
```

---

### Policy Types — Managed vs Customer Managed vs Inline

There are three types of policy in IAM:

|                      | AWS Managed          | Customer Managed    | Inline                 |
| -------------------- | -------------------- | ------------------- | ---------------------- |
| Written by           | AWS                  | You                 | You                    |
| Reusable             | ✅                   | ✅                  | ✗ One user only        |
| Versioning / history | ✅                   | ✅                  | ✗                      |
| Best for             | Quick broad access   | Shared custom rules | One-off specific rules |
| Example              | `AmazonS3FullAccess` | `my-app-s3-access`  | Directly on one user   |

**AWS managed** policies like `AmazonS3FullAccess` grant access to _every bucket in your account_ — far too broad for an app that only needs one bucket.

**Customer managed** policies are written by you but saved as a standalone reusable object in IAM. You can attach the same policy to as many users, groups, or roles as you want. If you update the policy, the change applies everywhere it's attached — no need to update each user individually.

**Inline policies** are also written by you but live directly on a single user. They cannot be reused and disappear when the user is deleted. Best for truly one-off rules that should never apply to anyone else.

### Creating and Attaching a Customer Managed Policy

```bash
# 1. Create the reusable policy in IAM
aws iam create-policy \
  --policy-name my-app-s3-access \
  --policy-document file://my-app-s3-policy.json

# Returns an ARN like:
# arn:aws:iam::179100901213:policy/my-app-s3-access
#                            ^^^^^^ your account, not "aws"
```

```bash
# 2. Attach it to as many users as needed — same command as AWS managed policies
aws iam attach-user-policy \
  --user-name my-app-s3-user \
  --policy-arn arn:aws:iam::179100901213:policy/my-app-s3-access

aws iam attach-user-policy \
  --user-name another-app-s3-user \
  --policy-arn arn:aws:iam::179100901213:policy/my-app-s3-access
```

The key difference from inline: the policy exists as its own standalone object in IAM, not embedded inside a user. You can list all your customer managed policies with:

```bash
aws iam list-policies --scope Local
# --scope Local = yours only, excludes AWS managed policies
```

### When to Use Which

- **AWS managed** — provisioner/admin users, broad service access, quick setup
- **Customer managed** — multiple apps or users that share the same access rules; update once, applies everywhere
- **Inline** — truly one-off rules tied to a single user that should never be reused

---

### Setting Up a Project User — S3 Example

#### 1. Create the user (run as provisioner)

```bash
aws iam create-user --user-name my-app-s3-user
```

#### 2. Create an inline policy (save as `my-app-s3-policy.json`)

This example allows the app to list, read, write, and delete objects in `my-app-bucket` only — nothing else. Omit `s3:DeleteObject` unless the app has a real cleanup or overwrite workflow that needs it.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-app-bucket"
    },
    {
      "Sid": "ReadWriteObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-app-bucket/*"
    }
  ]
}
```

Note the two separate resource ARNs:

- `arn:aws:s3:::my-app-bucket` — the bucket itself (needed for `ListBucket`)
- `arn:aws:s3:::my-app-bucket/*` — the objects inside it (needed for get/put/delete)

#### 3. Attach the inline policy to the user

```bash
aws iam put-user-policy \
  --user-name my-app-s3-user \
  --policy-name my-app-s3-access \
  --policy-document file://my-app-s3-policy.json
```

Note: inline policies use `put-user-policy`, not `attach-user-policy` (which is for managed policies).

#### 4. Create access keys for the app

```bash
aws iam create-access-key --user-name my-app-s3-user
```

Store these in your app's environment variables or secrets manager — never hardcoded in source code:

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-central-1
```

#### 5. Verify the policy is correct

```bash
aws iam list-user-policies --user-name my-app-s3-user
# Should show: my-app-s3-access

aws iam get-user-policy \
  --user-name my-app-s3-user \
  --policy-name my-app-s3-access
# Returns the full policy JSON
```

---

## Recommended Setup for a New AWS Account

```text
1. Log in as root (one-time only)
   ├── Enable IAM Identity Center
   ├── Enable MFA on root
   └── Create provisioner user in Identity Center

2. From now on, always use provisioner
   ├── Console: login via IAM Identity Center portal
   └── CLI: aws sso login --profile provisioner

3. For each project/app
   └── Provisioner creates a scoped IAM user
         ├── Custom inline policy (least-privilege)
         ├── Access keys stored in app env vars / secrets manager
         └── No console access, no MFA needed (not a human user)
```
