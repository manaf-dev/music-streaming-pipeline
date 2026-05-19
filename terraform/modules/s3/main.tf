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
      Project     = var.project_name
      Environment = var.env
      ManagedBy   = "terraform"
    },
    var.tags,
  )
}

# ---------------------------------------------------------------------------
# Bucket
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "main" {
  bucket        = var.bucket_name
  force_destroy = false

  tags = local.common_tags
}

# Encrypt every object at rest with SSE-S3 (AES-256, no extra cost).
# KMS not required for this project; the dataset is non-PII test data.
resource "aws_s3_bucket_server_side_encryption_configuration" "main" {
  bucket = aws_s3_bucket.main.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block every form of public access — bucket is private by design.
resource "aws_s3_bucket_public_access_block" "main" {
  bucket = aws_s3_bucket.main.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versioning protects against accidental deletion / overwrite of reference data.
resource "aws_s3_bucket_versioning" "main" {
  bucket = aws_s3_bucket.main.id

  versioning_configuration {
    status = "Enabled"
  }
}

# CRITICAL: enable S3 → EventBridge event delivery. Without this resource no
# ObjectCreated events ever reach EventBridge, so the entire pipeline stays
# silent. The Step Functions trigger rule in the step_functions module relies
# on this being true.
resource "aws_s3_bucket_notification" "eventbridge" {
  bucket      = aws_s3_bucket.main.id
  eventbridge = true
}

# ---------------------------------------------------------------------------
# Lifecycle — clean up raw/ after archival; tier archive/ down to IA after 90d
# ---------------------------------------------------------------------------
resource "aws_s3_bucket_lifecycle_configuration" "main" {
  bucket = aws_s3_bucket.main.id

  depends_on = [aws_s3_bucket_versioning.main]

  rule {
    id     = "expire-raw"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    expiration {
      days = var.raw_expiration_days
    }

    noncurrent_version_expiration {
      noncurrent_days = var.raw_expiration_days
    }
  }

  rule {
    id     = "transition-archive"
    status = "Enabled"

    filter {
      prefix = "archive/"
    }

    transition {
      days          = var.archive_transition_days
      storage_class = "STANDARD_IA"
    }
  }
}

# ---------------------------------------------------------------------------
# Reference data upload — songs and users CSVs into reference/ prefix
# ---------------------------------------------------------------------------
resource "aws_s3_object" "songs_reference" {
  bucket       = aws_s3_bucket.main.id
  key          = "reference/songs/songs.csv"
  source       = "${var.data_dir}/songs/songs.csv"
  etag         = filemd5("${var.data_dir}/songs/songs.csv")
  content_type = "text/csv"

  tags = local.common_tags
}

resource "aws_s3_object" "users_reference" {
  bucket       = aws_s3_bucket.main.id
  key          = "reference/users/users.csv"
  source       = "${var.data_dir}/users/users.csv"
  etag         = filemd5("${var.data_dir}/users/users.csv")
  content_type = "text/csv"

  tags = local.common_tags
}
