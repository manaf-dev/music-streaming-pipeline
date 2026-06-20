output "validate_role_arn" {
  description = "ARN of the Glue validate_schema job role."
  value       = aws_iam_role.glue_validate.arn
}

output "transform_role_arn" {
  description = "ARN of the Glue transform_kpis job role."
  value       = aws_iam_role.glue_transform.arn
}

output "ingest_role_arn" {
  description = "ARN of the Glue ingest_to_dynamodb job role."
  value       = aws_iam_role.glue_ingest.arn
}

output "sfn_role_arn" {
  description = "ARN of the Step Functions state-machine role."
  value       = aws_iam_role.step_functions.arn
}

output "eventbridge_role_arn" {
  description = "ARN of the EventBridge rule target role."
  value       = aws_iam_role.eventbridge.arn
}

output "job_arns" {
  description = "Pre-computed Glue job ARNs by short name (validate, transform, ingest)."
  value       = local.job_arns
}
