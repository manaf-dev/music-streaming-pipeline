variable "project_name" {
  description = "Short project identifier used in tags and resource names."
  type        = string
  default     = "music-streaming-pipeline"
}

variable "sfn_role_arn" {
  description = "IAM role ARN that the state machine assumes during execution."
  type        = string
}

variable "eventbridge_role_arn" {
  description = "IAM role ARN that the EventBridge rule assumes to call states:StartExecution."
  type        = string
}

variable "glue_job_names" {
  description = "Map of Glue job short names to Glue job names (validate, transform, ingest)."
  type = object({
    validate  = string
    transform = string
    ingest    = string
  })
}

variable "sns_alert_email" {
  description = "Email address subscribed to the pipeline-alerts SNS topic (operator must click confirm link on first deploy)."
  type        = string
}

variable "table_name" {
  description = "DynamoDB KPI table name (injected into the ingest job arguments by ASL templatefile)."
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name — the EventBridge rule filters on this bucket for ObjectCreated events."
  type        = string
}

variable "asl_template_path" {
  description = "Absolute path to src/step_functions/state_machine.asl.json."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the state machine log group."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Additional tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
