variable "env" {
  description = "Deployment environment (dev, prod). Used in role names and resource ARN construction."
  type        = string
}

variable "project_name" {
  description = "Short project identifier used in tags and resource names."
  type        = string
  default     = "music-streaming-pipeline"
}

variable "region" {
  description = "AWS region where Glue jobs and Step Functions state machine live (used to construct ARNs)."
  type        = string
}

variable "account_id" {
  description = "AWS account id (used to construct Glue job, state machine, and SNS topic ARNs)."
  type        = string
}

variable "bucket_arn" {
  description = "S3 bucket ARN passed from the s3 module. Policies scope object-level permissions under this ARN."
  type        = string
}

variable "table_arn" {
  description = "DynamoDB table ARN passed from the dynamodb module. The ingest role can BatchWriteItem on this ARN only."
  type        = string
}

variable "state_machine_arn" {
  description = "Predicted Step Functions state machine ARN. Computed in the root module from region + account + name."
  type        = string
}

variable "sns_topic_arn" {
  description = "Predicted SNS pipeline-alerts topic ARN. Computed in the root module from region + account + name."
  type        = string
}

variable "tags" {
  description = "Additional tags applied to every role and policy."
  type        = map(string)
  default     = {}
}
