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
  effective_table_name = coalesce(var.table_name, "${var.env}-music-streaming-kpis")

  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.env
      ManagedBy   = "terraform"
    },
    var.tags,
  )
}

# Single-table design — pk + sk distinguish all three KPI item types
# (GENRE_KPI#, TOP_SONGS#, TOP_GENRES#) via composite key prefixes.
# PAY_PER_REQUEST avoids capacity planning for unpredictable batch loads;
# PITR enables recovery for the prior 35 days; TTL on expires_at lets us
# auto-prune stale KPIs without a separate cleanup job.
resource "aws_dynamodb_table" "kpis" {
  name                        = local.effective_table_name
  billing_mode                = "PAY_PER_REQUEST"
  hash_key                    = "pk"
  range_key                   = "sk"
  deletion_protection_enabled = var.deletion_protection_enabled

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = var.ttl_attribute
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = local.common_tags
}
