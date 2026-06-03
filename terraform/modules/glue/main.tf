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
    time = {
      source  = "hashicorp/time"
      version = ">= 0.9"
    }
  }
}

locals {
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.env
      ManagedBy   = "terraform"
    },
    var.tags,
  )

  job_names = {
    validate  = "${var.env}-validate-schema"
    transform = "${var.env}-transform-kpis"
    ingest    = "${var.env}-ingest-to-dynamodb"
    archive   = "${var.env}-archive-files"
  }

  # Script filenames inside var.glue_scripts_dir — uploaded individually to S3.
  script_files = {
    validate  = "validate_schema.py"
    transform = "transform_kpis.py"
    ingest    = "ingest_to_dynamodb.py"
    archive   = "archive_files.py"
  }

  scripts_prefix = "glue-assets/scripts"
  utils_zip_key  = "glue-assets/utils.zip"
  temp_prefix    = "glue-temp"

  utils_zip_s3_uri = "s3://${var.bucket_name}/${local.utils_zip_key}"
}

# ---------------------------------------------------------------------------
# Package src/utils/ as utils.zip on every plan/apply.
# Glue passes this via --extra-py-files so import src.utils.* works inside
# both Python Shell and PySpark scripts.
# ---------------------------------------------------------------------------
data "archive_file" "utils_zip" {
  type        = "zip"
  source_dir  = var.utils_source_dir
  output_path = "${path.module}/utils.zip"
}

resource "aws_s3_object" "utils_zip" {
  bucket      = var.bucket_name
  key         = local.utils_zip_key
  source      = data.archive_file.utils_zip.output_path
  source_hash = data.archive_file.utils_zip.output_md5
  etag        = data.archive_file.utils_zip.output_md5
  tags        = local.common_tags
}

# ---------------------------------------------------------------------------
# Upload each Glue script to s3://bucket/glue-assets/scripts/<name>.py
# ---------------------------------------------------------------------------
resource "aws_s3_object" "scripts" {
  for_each = local.script_files

  bucket       = var.bucket_name
  key          = "${local.scripts_prefix}/${each.value}"
  source       = "${var.glue_scripts_dir}/${each.value}"
  etag         = filemd5("${var.glue_scripts_dir}/${each.value}")
  content_type = "text/x-python"
  tags         = local.common_tags
}

# ---------------------------------------------------------------------------
# CloudWatch log groups — one per job, explicit so retention can be controlled
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "jobs" {
  for_each = local.job_names

  name              = "/aws-glue/jobs/${each.value}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

# ---------------------------------------------------------------------------
# IAM role propagation buffer.
# glue:CreateJob validates that the passed role is assumable by Glue. A role
# created moments earlier may not have propagated yet, so a fresh `apply`
# (e.g. a brand-new environment) can fail with InvalidInputException. This
# sleep gates job creation on the role ARNs and adds a propagation buffer;
# it re-triggers only when the role ARNs change.
# ---------------------------------------------------------------------------
resource "time_sleep" "role_propagation" {
  create_duration = "30s"

  triggers = {
    role_arns = join(",", [
      var.validate_role_arn,
      var.transform_role_arn,
      var.ingest_role_arn,
      var.archive_role_arn,
    ])
  }
}

# ===========================================================================
# Glue Python Shell — validate_schema
# ===========================================================================
resource "aws_glue_job" "validate" {
  depends_on = [time_sleep.role_propagation]

  name         = local.job_names.validate
  role_arn     = var.validate_role_arn
  glue_version = "3.0"
  max_capacity = 0.0625
  max_retries  = 0 # retries are owned by Step Functions, not Glue
  timeout      = 10
  execution_property {
    max_concurrent_runs = 5 # tolerate concurrent file uploads
  }

  command {
    name            = "pythonshell"
    script_location = "s3://${var.bucket_name}/${aws_s3_object.scripts["validate"].key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--extra-py-files"                   = local.utils_zip_s3_uri
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.jobs["validate"].name
    "--job-language"                     = "python"
  }

  tags = local.common_tags
}

# ===========================================================================
# Glue ETL (PySpark) — transform_kpis
# ===========================================================================
resource "aws_glue_job" "transform" {
  depends_on        = [time_sleep.role_propagation]
  name              = local.job_names.transform
  role_arn          = var.transform_role_arn
  glue_version      = "4.0"
  worker_type       = var.transform_worker_type
  number_of_workers = var.transform_number_of_workers
  max_retries       = 0
  timeout           = 30
  execution_property {
    max_concurrent_runs = 5
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/${aws_s3_object.scripts["transform"].key}"
    python_version  = "3"
  }

  # NOTE: --enable-metrics MUST be the empty string, not "true". Per the Glue
  # documentation, the presence of the flag with any non-empty value disables
  # metrics in certain Glue versions; "" is the documented enable-form.
  default_arguments = {
    "--extra-py-files"                   = local.utils_zip_s3_uri
    "--enable-metrics"                   = ""
    "--enable-spark-ui"                  = "false"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${var.bucket_name}/${local.temp_prefix}/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.jobs["transform"].name
    "--job-language"                     = "python"
  }

  tags = local.common_tags
}

# ===========================================================================
# Glue Python Shell — ingest_to_dynamodb
# ===========================================================================
resource "aws_glue_job" "ingest" {
  depends_on   = [time_sleep.role_propagation]
  name         = local.job_names.ingest
  role_arn     = var.ingest_role_arn
  glue_version = "3.0"
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 30
  execution_property {
    max_concurrent_runs = 5
  }

  command {
    name            = "pythonshell"
    script_location = "s3://${var.bucket_name}/${aws_s3_object.scripts["ingest"].key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--extra-py-files"                   = local.utils_zip_s3_uri
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.jobs["ingest"].name
    "--job-language"                     = "python"
    # --table_name and --bucket/--execution_id are injected at runtime by Step Functions
  }

  tags = local.common_tags
}

# ===========================================================================
# Glue Python Shell — archive_files
# ===========================================================================
resource "aws_glue_job" "archive" {
  depends_on   = [time_sleep.role_propagation]
  name         = local.job_names.archive
  role_arn     = var.archive_role_arn
  glue_version = "3.0"
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 10
  execution_property {
    max_concurrent_runs = 5
  }

  command {
    name            = "pythonshell"
    script_location = "s3://${var.bucket_name}/${aws_s3_object.scripts["archive"].key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--extra-py-files"                   = local.utils_zip_s3_uri
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.jobs["archive"].name
    "--job-language"                     = "python"
  }

  tags = local.common_tags
}
