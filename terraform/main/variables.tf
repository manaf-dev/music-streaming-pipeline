variable "region" {
  description = "AWS region for all resources."
  type        = string
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for pipeline data."
  type        = string
}

variable "sns_alert_email" {
  description = "Email address for the pipeline-alerts SNS subscription."
  type        = string
}

variable "project_name" {
  description = "Short project identifier used in resource names and tags."
  type        = string
  default     = "music-streaming-pipeline"
}
