output "bucket_name" {
  description = "S3 bucket name — referenced by upload_sample_data.sh."
  value       = module.s3.bucket_name
}

output "table_name" {
  description = "DynamoDB KPI table name — referenced by query_dynamodb.sh."
  value       = module.dynamodb.table_name
}

output "state_machine_arn" {
  description = "Step Functions state machine ARN — useful for ad-hoc start-execution calls."
  value       = module.step_functions.state_machine_arn
}

output "sns_topic_arn" {
  description = "Pipeline-alerts SNS topic ARN."
  value       = module.step_functions.sns_topic_arn
}

output "region" {
  description = "AWS region of the deployment."
  value       = var.region
}
