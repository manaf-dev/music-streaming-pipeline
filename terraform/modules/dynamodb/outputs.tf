output "table_name" {
  description = "DynamoDB table name."
  value       = aws_dynamodb_table.kpis.name
}

output "table_arn" {
  description = "DynamoDB table ARN — pass to IAM module for least-privilege policies."
  value       = aws_dynamodb_table.kpis.arn
}

output "ttl_attribute" {
  description = "Name of the attribute used for TTL eviction."
  value       = var.ttl_attribute
}
