terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4"
    }
  }

  backend "s3" {
    key          = "prod/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.env
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  state_machine_arn = "arn:aws:states:${var.region}:${local.account_id}:stateMachine:${var.env}-music-streaming-pipeline"
  sns_topic_arn     = "arn:aws:sns:${var.region}:${local.account_id}:${var.env}-pipeline-alerts"

  repo_root        = abspath("${path.module}/../../..")
  data_dir         = "${local.repo_root}/data"
  utils_source_dir = "${local.repo_root}/src/utils"
  glue_scripts_dir = "${local.repo_root}/src/glue_jobs"
  asl_path         = "${local.repo_root}/src/step_functions/state_machine.asl.json"
}

module "s3" {
  source = "../../modules/s3"

  bucket_name  = var.bucket_name
  env          = var.env
  project_name = var.project_name
  data_dir     = local.data_dir
}

module "dynamodb" {
  source = "../../modules/dynamodb"

  env                         = var.env
  project_name                = var.project_name
  deletion_protection_enabled = true # prod: require manual disable before destroy
}

module "iam" {
  source = "../../modules/iam"

  env               = var.env
  project_name      = var.project_name
  region            = var.region
  account_id        = local.account_id
  bucket_arn        = module.s3.bucket_arn
  table_arn         = module.dynamodb.table_arn
  state_machine_arn = local.state_machine_arn
  sns_topic_arn     = local.sns_topic_arn
}

module "glue" {
  source = "../../modules/glue"

  env                = var.env
  project_name       = var.project_name
  bucket_name        = module.s3.bucket_name
  bucket_arn         = module.s3.bucket_arn
  validate_role_arn  = module.iam.validate_role_arn
  transform_role_arn = module.iam.transform_role_arn
  ingest_role_arn    = module.iam.ingest_role_arn
  archive_role_arn   = module.iam.archive_role_arn
  table_name         = module.dynamodb.table_name
  utils_source_dir   = local.utils_source_dir
  glue_scripts_dir   = local.glue_scripts_dir
}

module "step_functions" {
  source = "../../modules/step_functions"

  env                  = var.env
  project_name         = var.project_name
  sfn_role_arn         = module.iam.sfn_role_arn
  eventbridge_role_arn = module.iam.eventbridge_role_arn
  glue_job_names = {
    validate  = module.glue.validate_job_name
    transform = module.glue.transform_job_name
    ingest    = module.glue.ingest_job_name
    archive   = module.glue.archive_job_name
  }
  sns_alert_email   = var.sns_alert_email
  table_name        = module.dynamodb.table_name
  bucket_name       = module.s3.bucket_name
  asl_template_path = local.asl_path
}
