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

State is **local** here — there is no S3 backend yet, that's exactly what
this run is bootstrapping. Commit `terraform.tfstate` to a secure store
(SSM Parameter Store, 1Password, Vault) if you need to re-apply later.

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

## Re-running

- If you change `github_org` / `github_repo`, re-apply to update the
  trust policies. The roles already exist, so this is an `update` plan.
- If your account already has a GitHub OIDC provider (created by a different
  project), set `create_oidc_provider = false` in `terraform.tfvars` so this
  module references the existing provider instead of trying to re-create it.
