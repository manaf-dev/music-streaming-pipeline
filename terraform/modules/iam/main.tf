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
  common_tags = merge(
    {
      Project   = var.project_name
      ManagedBy = "terraform"
    },
    var.tags,
  )

  # Glue job ARNs — constructed deterministically so IAM can be created before
  # the glue module exists (avoids circular dependency).
  job_names = {
    validate  = "${var.project_name}-validate-schema"
    transform = "${var.project_name}-transform-kpis"
    ingest    = "${var.project_name}-ingest-to-dynamodb"
  }
  job_arns = {
    for k, name in local.job_names :
    k => "arn:aws:glue:${var.region}:${var.account_id}:job/${name}"
  }
  all_job_arns = values(local.job_arns)

  # CloudWatch Logs paths under /aws-glue/* are not known at plan time.
  glue_log_groups_arn = "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws-glue/*"
}

# ===========================================================================
# Trust policies — who is allowed to assume each role?
# ===========================================================================
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

# ===========================================================================
# Shared CloudWatch Logs statement — every Glue job needs it
# ===========================================================================
data "aws_iam_policy_document" "glue_cloudwatch_logs" {
  statement {
    sid    = "AllowGlueCloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:AssociateKmsKey",
    ]
    resources = [local.glue_log_groups_arn]
  }
}

# ===========================================================================
# Glue: validate_schema — read raw/ only
# ===========================================================================
resource "aws_iam_role" "glue_validate" {
  name               = "${var.project_name}-glue-validate-schema"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "glue_validate" {
  source_policy_documents = [data.aws_iam_policy_document.glue_cloudwatch_logs.json]

  statement {
    sid       = "ReadRawStreams"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arn}/raw/*"]
  }

  # HeadObject (via GetObject) on static reference CSVs before processing streams.
  statement {
    sid       = "HeadReferenceData"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arn}/reference/*"]
  }

  statement {
    sid       = "ListBucketForExtraPyFiles"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
  }

  statement {
    sid       = "ReadGlueAssets"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arn}/glue-assets/*"]
  }
}

resource "aws_iam_role_policy" "glue_validate" {
  name   = "${var.project_name}-glue-validate-schema-policy"
  role   = aws_iam_role.glue_validate.id
  policy = data.aws_iam_policy_document.glue_validate.json
}

# ===========================================================================
# Glue: transform_kpis — read raw/ + reference/, write processed/
# ===========================================================================
resource "aws_iam_role" "glue_transform" {
  name               = "${var.project_name}-glue-transform-kpis"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "glue_transform" {
  source_policy_documents = [data.aws_iam_policy_document.glue_cloudwatch_logs.json]

  statement {
    sid       = "ReadRawAndReference"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arn}/raw/*", "${var.bucket_arn}/reference/*"]
  }

  # awswrangler to_parquet(mode="overwrite") needs ListBucket + ListObjectsV2
  # on the bucket itself and DeleteObject on processed/* to remove the old
  # snapshot before writing the new one. Without these the transform fails
  # silently with a half-written processed/ prefix.
  statement {
    sid       = "ListBucketForOverwrite"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:ListBucketVersions"]
    resources = [var.bucket_arn]
  }

  # Native Spark Parquet output also writes zero-byte "<dir>_$folder$" markers
  # (e.g. the bucket-root key "processed_$folder$"), which do not match a
  # "processed/*" pattern. Use the "processed*" prefix so both the data objects
  # and the folder markers are writable.
  statement {
    sid       = "WriteProcessedKPIs"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:GetObject", "s3:AbortMultipartUpload"]
    resources = ["${var.bucket_arn}/processed*"]
  }

  statement {
    sid       = "ReadGlueAssets"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arn}/glue-assets/*"]
  }

  # Glue 4.0 PySpark uses a TempDir for shuffle spill / driver outputs.
  statement {
    sid    = "GlueTempDir"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${var.bucket_arn}/glue-temp*"]
  }
}

resource "aws_iam_role_policy" "glue_transform" {
  name   = "${var.project_name}-glue-transform-kpis-policy"
  role   = aws_iam_role.glue_transform.id
  policy = data.aws_iam_policy_document.glue_transform.json
}

# ===========================================================================
# Glue: ingest_to_dynamodb — read processed/, write DynamoDB
# ===========================================================================
resource "aws_iam_role" "glue_ingest" {
  name               = "${var.project_name}-glue-ingest-to-dynamodb"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "glue_ingest" {
  source_policy_documents = [data.aws_iam_policy_document.glue_cloudwatch_logs.json]

  statement {
    sid       = "ReadProcessedParquet"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arn}/processed/*"]
  }

  statement {
    sid       = "ListProcessedForAwswrangler"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
  }

  statement {
    sid       = "ReadGlueAssets"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arn}/glue-assets/*"]
  }

  statement {
    sid    = "ReadWriteKPIs"
    effect = "Allow"
    actions = [
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      # Query is required to recompute served KPIs from the per-execution
      # partials (GENRE_PARTIAL / SONG_PARTIAL / GENRECOUNT) during merge.
      "dynamodb:Query",
      "dynamodb:DescribeTable",
    ]
    resources = [var.table_arn]
  }
}

resource "aws_iam_role_policy" "glue_ingest" {
  name   = "${var.project_name}-glue-ingest-to-dynamodb-policy"
  role   = aws_iam_role.glue_ingest.id
  policy = data.aws_iam_policy_document.glue_ingest.json
}

# ===========================================================================
# Step Functions — start/manage Glue jobs, archive via S3 SDK, publish to SNS
# ===========================================================================
resource "aws_iam_role" "step_functions" {
  name               = "${var.project_name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "step_functions" {
  # Step Functions .sync integration requires ALL FOUR actions; missing any
  # one of them causes the StartJobRun.sync to hang indefinitely until the
  # state-machine timeout fires.
  statement {
    sid    = "InvokeGlueJobs"
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = local.all_job_arns
  }

  statement {
    sid       = "PublishPipelineAlerts"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }

  statement {
    sid    = "ArchiveProcessedStreams"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${var.bucket_arn}/raw/*",
      "${var.bucket_arn}/archive/*",
    ]
  }

  # CloudWatch Logs for the state machine's execution logs.
  statement {
    sid    = "StateMachineLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "${var.project_name}-sfn-policy"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}

# ===========================================================================
# EventBridge — start the state machine on S3 raw/streams/ ObjectCreated
# ===========================================================================
resource "aws_iam_role" "eventbridge" {
  name               = "${var.project_name}-eventbridge"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "eventbridge" {
  statement {
    sid       = "StartStateMachine"
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [var.state_machine_arn]
  }
}

resource "aws_iam_role_policy" "eventbridge" {
  name   = "${var.project_name}-eventbridge-policy"
  role   = aws_iam_role.eventbridge.id
  policy = data.aws_iam_policy_document.eventbridge.json
}
