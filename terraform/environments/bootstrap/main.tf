terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }

  # Local state on purpose — this env runs ONCE by a human before any CI
  # exists, and creates the IAM roles that CI later assumes. There's no
  # S3 backend to write to until *after* this apply succeeds.
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
      Purpose   = "github-actions-oidc"
    }
  }
}

module "github_oidc" {
  source = "../../modules/github_oidc"

  github_org           = var.github_org
  github_repo          = var.github_repo
  project_name         = var.project_name
  create_oidc_provider = var.create_oidc_provider
}

# ---------------------------------------------------------------------------
# Terraform state bucket — shared by dev and prod environments.
# Keys land at:
#   s3://<bucket>/dev/terraform.tfstate
#   s3://<bucket>/prod/terraform.tfstate
# Each env's `terraform init` is passed -backend-config="bucket=..." so this
# bucket name (output below) is the value of the TF_STATE_BUCKET GitHub var.
# ---------------------------------------------------------------------------
data "aws_caller_identity" "current" {}

locals {
  tfstate_bucket_name = coalesce(
    var.tfstate_bucket_name,
    "${var.project_name}-tfstate-${data.aws_caller_identity.current.account_id}",
  )
}

resource "aws_s3_bucket" "tfstate" {
  bucket = local.tfstate_bucket_name

  # Refuse a `terraform destroy` while the bucket still has objects in it.
  # Force-emptying state history is a deliberate two-step.
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Keep storage costs bounded: prune non-current state versions after 90 days.
resource "aws_s3_bucket_lifecycle_configuration" "tfstate" {
  bucket     = aws_s3_bucket.tfstate.id
  depends_on = [aws_s3_bucket_versioning.tfstate]

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.tfstate_noncurrent_version_retention_days
    }

    # Clean up partial uploads that never finished.
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
