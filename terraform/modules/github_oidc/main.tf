terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

locals {
  common_tags = merge(
    {
      Project   = var.project_name
      ManagedBy = "terraform"
      Purpose   = "github-actions-oidc"
    },
    var.tags,
  )

  # Subjects authorised for each role:
  #   ref:refs/heads/<branch>          — push/PR runs on the target branch
  #   environment:<env-name>           — jobs that declare `environment: <env-name>`
  # Both forms are needed because the GitHub OIDC `sub` claim flips between
  # them depending on what kind of job is running.
  dev_subjects = [
    "repo:${var.github_org}/${var.github_repo}:ref:refs/heads/${var.dev_branch}",
    "repo:${var.github_org}/${var.github_repo}:environment:${var.dev_environment_name}",
    "repo:${var.github_org}/${var.github_repo}:pull_request",
  ]
  prod_subjects = [
    "repo:${var.github_org}/${var.github_repo}:ref:refs/heads/${var.prod_branch}",
    "repo:${var.github_org}/${var.github_repo}:environment:${var.prod_environment_name}",
  ]
}

# ===========================================================================
# OIDC provider for GitHub Actions (account-global, exists once)
# ===========================================================================
resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # GitHub's thumbprint list is now ignored by AWS — present here for backward
  # compatibility with older Terraform validators. Use a sentinel value.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]

  tags = local.common_tags
}

data "aws_iam_openid_connect_provider" "github_existing" {
  count = var.create_oidc_provider ? 0 : 1
  url   = "https://token.actions.githubusercontent.com"
}

locals {
  oidc_provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : data.aws_iam_openid_connect_provider.github_existing[0].arn
}

# ===========================================================================
# Trust policies
# ===========================================================================
data "aws_iam_policy_document" "dev_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = local.dev_subjects
    }
  }
}

data "aws_iam_policy_document" "prod_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = local.prod_subjects
    }
  }
}

# ===========================================================================
# Deploy permissions — broad enough to manage all resources Terraform creates
# for this project, scoped to actions (not "*") so the role can't reach
# unrelated services.
# ===========================================================================
data "aws_iam_policy_document" "deploy_permissions" {
  # Terraform's S3 backend + every S3 resource the project manages.
  statement {
    sid    = "S3"
    effect = "Allow"
    actions = [
      # Broad read: the AWS provider refreshes many bucket sub-resources
      # (accelerate, acl, cors, logging, website, analytics, metrics, …) whose
      # IAM action names are NOT all under the s3:GetBucket* prefix — e.g.
      # s3:GetAccelerateConfiguration. Granting s3:Get*/s3:List* avoids
      # whack-a-mole AccessDenied during plan/refresh.
      "s3:Get*",
      "s3:List*",
      # Writes — scoped to the bucket + object operations Terraform performs.
      "s3:CreateBucket",
      "s3:DeleteBucket",
      "s3:PutBucket*",
      "s3:PutEncryptionConfiguration",
      "s3:PutLifecycleConfiguration",
      "s3:PutObject*",
      "s3:DeleteObject*",
      "s3:CopyObject",
      "s3:AbortMultipartUpload",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "DynamoDB"
    effect = "Allow"
    actions = [
      "dynamodb:CreateTable",
      "dynamodb:DeleteTable",
      "dynamodb:DescribeTable",
      "dynamodb:UpdateTable",
      "dynamodb:UpdateTimeToLive",
      "dynamodb:DescribeTimeToLive",
      "dynamodb:UpdateContinuousBackups",
      "dynamodb:DescribeContinuousBackups",
      "dynamodb:TagResource",
      "dynamodb:UntagResource",
      "dynamodb:ListTagsOfResource",
    ]
    resources = ["*"]
  }

  # Pipeline per-job IAM roles + OIDC roles must be creatable here.
  statement {
    sid    = "IAM"
    effect = "Allow"
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:UpdateRole",
      "iam:UpdateRoleDescription",
      "iam:ListRoles",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:ListAttachedRolePolicies",
      "iam:PassRole",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:ListInstanceProfilesForRole",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "IAMOpenIDConnect"
    effect    = "Allow"
    actions   = ["iam:*OpenIDConnectProvider*"]
    resources = ["*"]
  }

  statement {
    sid    = "Glue"
    effect = "Allow"
    actions = [
      "glue:CreateJob",
      "glue:DeleteJob",
      "glue:UpdateJob",
      "glue:GetJob",
      "glue:GetJobs",
      "glue:BatchGetJobs",
      "glue:GetTags",
      "glue:TagResource",
      "glue:UntagResource",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "StepFunctions"
    effect = "Allow"
    actions = [
      "states:CreateStateMachine",
      "states:DeleteStateMachine",
      "states:UpdateStateMachine",
      "states:DescribeStateMachine",
      "states:ListStateMachines",
      "states:ListTagsForResource",
      "states:TagResource",
      "states:UntagResource",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "EventBridge"
    effect = "Allow"
    actions = [
      "events:PutRule",
      "events:DeleteRule",
      "events:DescribeRule",
      "events:ListRules",
      "events:EnableRule",
      "events:DisableRule",
      "events:PutTargets",
      "events:RemoveTargets",
      "events:ListTargetsByRule",
      "events:ListTagsForResource",
      "events:TagResource",
      "events:UntagResource",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "SNS"
    effect = "Allow"
    actions = [
      "sns:CreateTopic",
      "sns:DeleteTopic",
      "sns:GetTopicAttributes",
      "sns:SetTopicAttributes",
      "sns:Subscribe",
      "sns:Unsubscribe",
      "sns:ListSubscriptionsByTopic",
      "sns:GetSubscriptionAttributes",
      "sns:SetSubscriptionAttributes",
      "sns:ListTagsForResource",
      "sns:TagResource",
      "sns:UntagResource",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:DeleteLogGroup",
      "logs:DescribeLogGroups",
      "logs:PutRetentionPolicy",
      "logs:DeleteRetentionPolicy",
      "logs:ListTagsForResource",
      "logs:TagResource",
      "logs:UntagResource",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "STSCallerIdentity"
    effect = "Allow"
    actions = [
      "sts:GetCallerIdentity",
    ]
    resources = ["*"]
  }
}

# ===========================================================================
# Dev role
# ===========================================================================
resource "aws_iam_role" "dev" {
  name               = "${var.project_name}-github-actions-dev"
  description        = "Assumed by GitHub Actions for the ${var.dev_branch} branch / ${var.dev_environment_name} environment."
  assume_role_policy = data.aws_iam_policy_document.dev_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "dev_deploy" {
  name   = "${var.project_name}-github-actions-dev-deploy"
  role   = aws_iam_role.dev.id
  policy = data.aws_iam_policy_document.deploy_permissions.json
}

# ===========================================================================
# Prod role
# ===========================================================================
resource "aws_iam_role" "prod" {
  name               = "${var.project_name}-github-actions-prod"
  description        = "Assumed by GitHub Actions for the ${var.prod_branch} branch / ${var.prod_environment_name} environment."
  assume_role_policy = data.aws_iam_policy_document.prod_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "prod_deploy" {
  name   = "${var.project_name}-github-actions-prod-deploy"
  role   = aws_iam_role.prod.id
  policy = data.aws_iam_policy_document.deploy_permissions.json
}
