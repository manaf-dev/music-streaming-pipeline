# Bootstrap — GitHub OIDC + deploy roles

Run this **once per AWS account**, with your own AWS credentials, before the
`dev` and `prod` environments can deploy via CI/CD.

It creates:

- The GitHub Actions OIDC provider (`token.actions.githubusercontent.com`).
- Two IAM roles: one assumable by the `development` branch / environment,
  one by the `main` branch / `production` environment.
- A permissions policy on each role broad enough to manage every resource
  the per-env Terraform configurations create.
- The S3 bucket that the dev and prod environments use for Terraform remote
  state (versioned, SSE-S3 encrypted, public-access-blocked, with a
  90-day lifecycle on non-current versions).

State starts **local** here — there is no S3 backend yet, that's exactly
what this run is bootstrapping. After the first apply succeeds you can
migrate state into the bucket it just created (see "Migrate to remote
state" below) so future runs lock + version like the dev/prod envs.

## Usage

```bash
cd terraform/environments/bootstrap
cp terraform.tfvars.example terraform.tfvars
# Edit github_org / github_repo in terraform.tfvars

terraform init
terraform apply
```

After it succeeds, configure GitHub secrets / variables using the outputs:

```bash
# Outputs to copy into GitHub repo settings
terraform output dev_role_name        # -> AWS_ROLE_NAME_DEV   (repo variable)
terraform output prod_role_name       # -> AWS_ROLE_NAME_PROD  (repo variable)
terraform output tfstate_bucket_name  # -> TF_STATE_BUCKET     (repo variable)
```

Account id (for the `AWS_ACCOUNT_ID` secret):

```bash
aws sts get-caller-identity --query Account --output text
```

## Migrate to remote state (recommended after first apply)

The bootstrap env starts on local state because it can't reference a bucket
that doesn't exist yet. Once the first `terraform apply` has run and the
tfstate bucket is live, promote the bootstrap env to S3-backed state so it
behaves like dev / prod:

```bash
./migrate-state.sh
```

That script renames `backend.tf.disabled` -> `backend.tf` and runs
`terraform init -migrate-state` with the bucket name + region read from your
local state. Terraform copies `terraform.tfstate` into
`s3://<bucket>/bootstrap/terraform.tfstate` and from then on every apply
locks via S3 (`use_lockfile = true`).

After migration you can delete the local `terraform.tfstate` and
`terraform.tfstate.backup` files — they're no longer authoritative.

## Re-running

- If you change `github_org` / `github_repo`, re-apply to update the
  trust policies. The roles already exist, so this is an `update` plan.
- If your account already has a GitHub OIDC provider (created by a different
  project), set `create_oidc_provider = false` in `terraform.tfvars` so this
  module references the existing provider instead of trying to re-create it.
- The state bucket has `force_destroy = false`. A `terraform destroy` will
  fail until you empty the bucket of state versions first.
