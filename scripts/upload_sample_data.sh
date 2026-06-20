#!/usr/bin/env bash
# Upload a sample streams CSV to s3://<bucket>/raw/streams/, triggering the
# EventBridge -> Step Functions pipeline.
#
# Usage:  ./scripts/upload_sample_data.sh [filename]
#         make upload FILE=streams1.csv
#
# Bucket resolution (first match wins):
#   1. S3_BUCKET_NAME env var
#   2. bucket_name in terraform/main/terraform.tfvars
#   3. terraform output bucket_name (requires remote state access)

set -euo pipefail

FILENAME="${1:-streams1.csv}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_FILE="${REPO_ROOT}/data/streams/${FILENAME}"
ENV_DIR="${REPO_ROOT}/terraform/main"
TFVARS="${ENV_DIR}/terraform.tfvars"

if [[ ! -f "${SOURCE_FILE}" ]]; then
  echo "ERROR: source file not found: ${SOURCE_FILE}" >&2
  exit 1
fi

read_tfvar() {
  local key="$1"
  [[ -f "${TFVARS}" ]] || return 1
  grep -E "^[[:space:]]*${key}[[:space:]]*=" "${TFVARS}" | head -1 \
    | sed -E 's/.*=[[:space:]]*"([^"]+)".*/\1/'
}

resolve_bucket_name() {
  if [[ -n "${S3_BUCKET_NAME:-}" ]]; then
    echo "${S3_BUCKET_NAME}"
    return
  fi
  local from_tfvars
  from_tfvars="$(read_tfvar bucket_name || true)"
  if [[ -n "${from_tfvars}" ]]; then
    echo "${from_tfvars}"
    return
  fi
  terraform -chdir="${ENV_DIR}" output -raw bucket_name
}

resolve_region() {
  if [[ -n "${AWS_REGION:-}" ]]; then
    echo "${AWS_REGION}"
    return
  fi
  if [[ -n "${AWS_DEFAULT_REGION:-}" ]]; then
    echo "${AWS_DEFAULT_REGION}"
    return
  fi
  local from_tfvars
  from_tfvars="$(read_tfvar region || true)"
  if [[ -n "${from_tfvars}" ]]; then
    echo "${from_tfvars}"
    return
  fi
  echo "eu-central-1"
}

echo "Resolving bucket name..."
BUCKET_NAME="$(resolve_bucket_name)"

if [[ -z "${BUCKET_NAME}" ]]; then
  echo "ERROR: could not resolve bucket name — set S3_BUCKET_NAME or run 'make apply'." >&2
  exit 1
fi

DEST_KEY="raw/streams/${FILENAME}"

echo "Uploading ${SOURCE_FILE}"
echo "       -> s3://${BUCKET_NAME}/${DEST_KEY}"
aws s3 cp "${SOURCE_FILE}" "s3://${BUCKET_NAME}/${DEST_KEY}"

REGION="$(resolve_region)"
STATE_MACHINE_ARN="$(aws stepfunctions list-state-machines \
  --region "${REGION}" \
  --query "stateMachines[?name=='music-streaming-pipeline'].stateMachineArn | [0]" \
  --output text)"

echo
echo "Upload complete. The EventBridge rule should now have triggered a new"
echo "Step Functions execution. Check the AWS console or run:"
if [[ -n "${STATE_MACHINE_ARN}" && "${STATE_MACHINE_ARN}" != "None" ]]; then
  echo "  aws stepfunctions list-executions --state-machine-arn \\"
  echo "    \"${STATE_MACHINE_ARN}\" --region ${REGION}"
else
  echo "  aws stepfunctions list-executions --state-machine-arn <arn> --region ${REGION}"
fi
