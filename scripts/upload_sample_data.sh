#!/usr/bin/env bash
# Upload a sample streams CSV to s3://<bucket>/raw/streams/, triggering the
# EventBridge -> Step Functions pipeline.
#
# Usage:  ./scripts/upload_sample_data.sh [filename] [env]
#
#   filename  Streams CSV under data/streams/  (default: streams1.csv)
#   env       Terraform environment            (default: dev)
#
# The bucket name is read from `terraform output -raw bucket_name` in the
# matching environment directory, so the script never hardcodes account or
# bucket identifiers.

set -euo pipefail

FILENAME="${1:-streams1.csv}"
ENV="${2:-dev}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_FILE="${REPO_ROOT}/data/streams/${FILENAME}"
ENV_DIR="${REPO_ROOT}/terraform/environments/${ENV}"

if [[ ! -f "${SOURCE_FILE}" ]]; then
  echo "ERROR: source file not found: ${SOURCE_FILE}" >&2
  exit 1
fi

if [[ ! -d "${ENV_DIR}" ]]; then
  echo "ERROR: terraform environment directory not found: ${ENV_DIR}" >&2
  exit 1
fi

echo "Resolving bucket name from terraform output in ${ENV_DIR}..."
BUCKET_NAME="$(terraform -chdir="${ENV_DIR}" output -raw bucket_name)"

if [[ -z "${BUCKET_NAME}" ]]; then
  echo "ERROR: terraform output 'bucket_name' is empty — did you apply the ${ENV} env yet?" >&2
  exit 1
fi

DEST_KEY="raw/streams/${FILENAME}"

echo "Uploading ${SOURCE_FILE}"
echo "       -> s3://${BUCKET_NAME}/${DEST_KEY}"
aws s3 cp "${SOURCE_FILE}" "s3://${BUCKET_NAME}/${DEST_KEY}"

echo
echo "Upload complete. The EventBridge rule should now have triggered a new"
echo "Step Functions execution. Check the AWS console or run:"
echo "  aws stepfunctions list-executions --state-machine-arn \\"
echo "    \"$(terraform -chdir="${ENV_DIR}" output -raw state_machine_arn)\""
