terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

locals {
  state_machine_name = var.project_name
  sns_topic_name     = "${var.project_name}-alerts"
  event_rule_name    = "${var.project_name}-raw-upload"

  common_tags = merge(
    {
      Project   = var.project_name
      ManagedBy = "terraform"
    },
    var.tags,
  )
}

# ===========================================================================
# SNS pipeline-alerts topic + email subscription
# ===========================================================================
resource "aws_sns_topic" "alerts" {
  name = local.sns_topic_name
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.sns_alert_email

  # The subscription is created in 'PendingConfirmation' state — the operator
  # MUST click the confirmation link sent to var.sns_alert_email before any
  # alerts are delivered. Terraform cannot confirm the subscription on the
  # operator's behalf, so this is expected on first deploy.
}

# ===========================================================================
# CloudWatch log group for the state machine
# ===========================================================================
resource "aws_cloudwatch_log_group" "state_machine" {
  name              = "/aws/vendedlogs/states/${local.state_machine_name}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

# ===========================================================================
# Standard state machine — definition built from src/step_functions/*.asl.json
# ===========================================================================
resource "aws_sfn_state_machine" "pipeline" {
  name     = local.state_machine_name
  role_arn = var.sfn_role_arn
  type     = "STANDARD" # .sync Glue integration requires Standard

  definition = templatefile(var.asl_template_path, {
    validate_job_name  = var.glue_job_names.validate
    transform_job_name = var.glue_job_names.transform
    ingest_job_name    = var.glue_job_names.ingest
    sns_topic_arn      = aws_sns_topic.alerts.arn
    table_name         = var.table_name
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machine.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = true
  }

  tags = local.common_tags
}

# ===========================================================================
# EventBridge — fire the state machine on S3 ObjectCreated under raw/streams/
# ===========================================================================
resource "aws_cloudwatch_event_rule" "raw_upload" {
  name        = local.event_rule_name
  description = "Triggers the music streaming pipeline when a .csv streams file lands in raw/streams/. Reference data (songs/users) is static and does not trigger runs."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [var.bucket_name]
      }
      object = {
        key = [{ prefix = "raw/streams/", suffix = ".csv" }]
      }
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "raw_upload_to_sfn" {
  rule      = aws_cloudwatch_event_rule.raw_upload.name
  target_id = "${local.state_machine_name}-target"
  arn       = aws_sfn_state_machine.pipeline.arn
  role_arn  = var.eventbridge_role_arn

  # Default input transformer is fine — we pass through the EventBridge S3
  # event verbatim. The state machine reads $.detail.bucket.name and
  # $.detail.object.key from this input.

  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }
}
