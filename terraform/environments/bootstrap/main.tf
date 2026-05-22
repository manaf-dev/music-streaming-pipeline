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
