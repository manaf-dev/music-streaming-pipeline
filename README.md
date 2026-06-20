# Music Streaming Pipeline

A serverless AWS data pipeline that ingests batch CSV uploads of user streaming events, validates and transforms them into daily KPIs with PySpark, and serves the results from a single DynamoDB table for millisecond-latency reads.

The pipeline is **event-driven for streams only**: drop a `.csv` into `raw/streams/` and EventBridge wakes Step Functions, which orchestrates three AWS Glue jobs (validate → transform → ingest) plus native S3 archival tasks, and emails on any failure.

`songs.csv` and `users.csv` are **static reference data** — uploaded by Terraform at apply time and validated before each run. They do **not** trigger the pipeline.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [One-time AWS account setup](#one-time-aws-account-setup)
- [Infrastructure setup](#infrastructure-setup)
- [Pipeline components](#pipeline-components)
- [KPIs computed](#kpis-computed)
- [DynamoDB table design & sample queries](#dynamodb-table-design--sample-queries)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Troubleshooting & logging](#troubleshooting--logging)

---

## Overview

Three input datasets exist in the project:

| Dataset        | Role in pipeline |
|----------------|------------------|
| `streams*.csv` | **Event-triggered** — each `.csv` upload under `raw/streams/` starts a run |
| `songs.csv`    | **Static reference** — Terraform uploads to `reference/songs/` |
| `users.csv`    | **Static reference** — Terraform uploads to `reference/users/` |

Actual listening duration is derived by joining streams against `songs.duration_ms` — `listen_time` is the playback **start** timestamp.

## Architecture

![Architecture diagram](docs/architecture.png)

**Trigger:** EventBridge `Object Created` on `raw/streams/*.csv` only. Reference data under `reference/` is joined at transform time and checked at validation — not upload-triggered.

**Flow:** EventBridge → Step Functions → Glue validate → Glue transform (PySpark) → Glue ingest → Step Functions S3 copy/delete to `archive/`.

## Prerequisites

| Tool      | Version        | Notes |
|-----------|----------------|-------|
| Python    | 3.12.x         | `uv python install 3.12` |
| uv        | 0.11+          | Package manager |
| Terraform | 1.15.x       | Tested with 1.15.5 |
| AWS provider | ~> 6.0    | [HashiCorp upgrade guide](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/guides/version-6-upgrade) |
| AWS CLI   | 2.x            | One-time account setup + smoke tests |
| Java      | 17 (headless)  | Required by PySpark locally |

---

## One-time AWS account setup

Run **once per AWS account** with administrator credentials before `make init`. No bootstrap Terraform module — use the AWS CLI.

Replace placeholders:

- `ACCOUNT_ID` — `aws sts get-caller-identity --query Account --output text`
- `REGION` — e.g. `eu-central-1`
- `GITHUB_ORG` / `GITHUB_REPO` — your GitHub coordinates
- `TFSTATE_BUCKET` — globally unique, e.g. `music-streaming-tfstate-ACCOUNT_ID`

### 1. Terraform remote state bucket

```bash
export REGION=eu-central-1
export TFSTATE_BUCKET=music-streaming-tfstate-$(aws sts get-caller-identity --query Account --output text)

aws s3api create-bucket \
  --bucket "$TFSTATE_BUCKET" \
  --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION"

aws s3api put-bucket-versioning \
  --bucket "$TFSTATE_BUCKET" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket "$TFSTATE_BUCKET" \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block \
  --bucket "$TFSTATE_BUCKET" \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

### 2. GitHub Actions OIDC provider

Skip if your account already has the provider:

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

### 3. GitHub Actions deploy role

```bash
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export GITHUB_ORG=your-org
export GITHUB_REPO=music-streaming-pipeline
export ROLE_NAME=music-streaming-pipeline-github-actions

sed -e "s/ACCOUNT_ID/${ACCOUNT_ID}/g" \
    -e "s/GITHUB_ORG/${GITHUB_ORG}/g" \
    -e "s/GITHUB_REPO/${GITHUB_REPO}/g" \
  scripts/github-actions-trust-policy.json > /tmp/gh-trust.json

aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/gh-trust.json \
  --description "GitHub Actions deploy role for music-streaming-pipeline"

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name music-streaming-pipeline-deploy \
  --policy-document file://scripts/github-actions-deploy-policy.json
```

### 4. GitHub repository configuration

| Type | Name | Value |
|------|------|-------|
| Secret | `AWS_ACCOUNT_ID` | Your 12-digit account ID |
| Secret | `SNS_ALERT_EMAIL` | Alert recipient email |
| Variable | `AWS_REGION` | e.g. `eu-central-1` |
| Variable | `TF_STATE_BUCKET` | `$TFSTATE_BUCKET` from step 1 |
| Variable | `S3_BUCKET_NAME` | Globally unique **data** bucket name |
| Variable | `AWS_ROLE_NAME` | `$ROLE_NAME` from step 3 |

Optional: create a GitHub **environment** named `production` for manual approval before CD apply (Settings → Environments).

---

## Infrastructure setup

```bash
cp terraform/main/terraform.tfvars.example terraform/main/terraform.tfvars
# edit bucket_name, region, sns_alert_email

export TF_STATE_BUCKET=your-tfstate-bucket
export AWS_REGION=eu-central-1

make init
make apply
```

After apply:

1. Confirm the SNS email subscription (click the link in the inbox).
2. Upload a stream batch: `make upload FILE=streams1.csv`
3. Watch executions: `aws stepfunctions list-executions --state-machine-arn $(terraform -chdir=terraform/main output -raw state_machine_arn)`

## Pipeline components

| Step | Type | What it does |
|------|------|--------------|
| `validate_schema` | Glue Python Shell | Confirms reference data exists; validates streams CSV columns, row count, `listen_time` |
| `transform_kpis` | Glue PySpark 4.0 | Broadcast-joins streams + songs + users; writes partial Parquet to `processed/<execution_id>/` |
| `ingest_to_dynamodb` | Glue Python Shell | Merges partials into daily KPIs in DynamoDB |
| `ArchiveCopyToS3` / `ArchiveDeleteRaw` | Step Functions S3 SDK | Copies `raw/` → `archive/`, deletes source (idempotent via Catch on `NoSuchKey`) |

## KPIs computed

Per `(track_genre, listen_date)` after merging all files for the day:

| KPI | Definition |
|-----|------------|
| `listen_count` | Total stream events |
| `unique_listeners` | Distinct users (union across files) |
| `total_listening_time_ms` | Sum of `duration_ms` |
| `avg_listening_time_per_user_ms` | `total / unique`, 2 dp |
| Top-3 songs per genre/day | By cumulative `play_count` |
| Top-5 genres per day | By cumulative `listen_count` |

## DynamoDB table design & sample queries

Single PAY_PER_REQUEST table; composite `pk` / `sk` prefixes:

| Item type | `pk` | `sk` |
|-----------|------|------|
| Genre KPI | `GENRE_KPI#<genre>#<date>` | `METADATA` |
| Top songs | `TOP_SONGS#<genre>#<date>` | `RANK#01` … |
| Top genres | `TOP_GENRES#<date>` | `RANK#01` … |

```bash
aws dynamodb get-item \
  --table-name music-streaming-kpis \
  --key '{"pk":{"S":"GENRE_KPI#pop#2024-01-15"},"sk":{"S":"METADATA"}}'
```

```bash
TABLE_NAME=music-streaming-kpis ./scripts/query_dynamodb.sh 2024-01-15
```

## Testing

CI runs ruff, mypy, pytest, and `terraform validate` on pull requests and deploys.

Locally (optional): `uv sync --all-extras && uv run pytest tests/`

## CI/CD

| Workflow | Trigger | Jobs |
|----------|---------|------|
| `ci.yml` | PR / push to `main` | ruff, mypy, pytest + integration, terraform validate |
| `cd.yml` | push to `main` | test gate → OIDC → `terraform apply` |

Authentication uses GitHub OIDC — no static AWS keys in the repository.

## Troubleshooting & logging

| Component | Log group |
|-----------|-----------|
| Glue validate | `/aws-glue/jobs/music-streaming-pipeline-validate-schema` |
| Glue transform | `/aws-glue/jobs/music-streaming-pipeline-transform-kpis` |
| Glue ingest | `/aws-glue/jobs/music-streaming-pipeline-ingest-to-dynamodb` |
| Step Functions | `/aws/vendedlogs/states/music-streaming-pipeline` |

**Pipeline never starts:** confirm `aws_s3_bucket_notification { eventbridge = true }` is applied on the data bucket and the uploaded key ends in `.csv`.

**Validate fails immediately:** run `make apply` — reference CSVs must exist at `reference/songs/songs.csv` and `reference/users/users.csv`.

**Archive step fails:** Step Functions role needs `s3:GetObject`/`PutObject`/`DeleteObject` on `raw/*` and `archive/*` (configured in the IAM module).
