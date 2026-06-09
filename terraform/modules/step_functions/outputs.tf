output "state_machine_arn" {
  description = "ARN of the Step Functions state machine."
  value       = aws_sfn_state_machine.pipeline.arn
}

output "state_machine_name" {
  description = "Name of the Step Functions state machine."
  value       = aws_sfn_state_machine.pipeline.name
}

output "sns_topic_arn" {
  description = "ARN of the pipeline-alerts SNS topic."
  value       = aws_sns_topic.alerts.arn
}

output "sns_topic_name" {
  description = "Name of the pipeline-alerts SNS topic."
  value       = aws_sns_topic.alerts.name
}

output "event_rule_name" {
  description = "Name of the EventBridge rule that triggers the state machine."
  value       = aws_cloudwatch_event_rule.raw_upload.name
}

output "state_machine_log_group" {
  description = "CloudWatch log group capturing state machine execution history."
  value       = aws_cloudwatch_log_group.state_machine.name
}
