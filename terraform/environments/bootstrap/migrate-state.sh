#!/usr/bin/env bash
# Promote the bootstrap env from local state to the S3 bucket it just created.
#
# Run this AFTER the initial `terraform apply` has succeeded and you've
# verified the state bucket exists. It:
#   1. Activates backend.tf (renames the .disabled stub)
#   2. Reads the bucket name and region from the current local state
#   3. Runs `terraform init -migrate-state` to copy state into S3
#
# Idempotent: re-running after migration is a no-op (terraform init detects
# the configured backend already matches and just refreshes plugins).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ -f backend.tf.disabled && ! -f backend.tf ]]; then
  echo "Activating backend.tf"
  mv backend.tf.disabled backend.tf
fi

BUCKET="$(terraform output -raw tfstate_bucket_name)"
REGION="$(awk -F'"' '/^region/ {print $2; exit}' terraform.tfvars 2>/dev/null || true)"
REGION="${REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"

if [[ -z "${BUCKET}" ]]; then
  echo "ERROR: tfstate_bucket_name output is empty. Did you run 'terraform apply' first?" >&2
  exit 1
fi
if [[ -z "${REGION}" ]]; then
  echo "ERROR: could not determine AWS region. Set 'region' in terraform.tfvars or export AWS_REGION." >&2
  exit 1
fi

echo "Migrating local state to s3://${BUCKET}/bootstrap/terraform.tfstate (region=${REGION})"

terraform init -migrate-state \
  -backend-config="bucket=${BUCKET}" \
  -backend-config="region=${REGION}"

echo
echo "Migration complete. The local terraform.tfstate is now safe to delete"
echo "(Terraform keeps a backup at terraform.tfstate.backup just in case)."
