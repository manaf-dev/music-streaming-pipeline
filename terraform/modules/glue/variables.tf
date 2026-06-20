variable "project_name" {
  description = "Short project identifier used in tags."
  type        = string
  default     = "music-streaming-pipeline"
}

variable "bucket_name" {
  description = "S3 bucket that hosts the Glue scripts, utils.zip, TempDir, and the raw/processed prefixes."
  type        = string
}

variable "bucket_arn" {
  description = "S3 bucket ARN — included in tags for traceability."
  type        = string
}

variable "validate_role_arn" {
  description = "IAM role ARN for the validate_schema Glue job."
  type        = string
}

variable "transform_role_arn" {
  description = "IAM role ARN for the transform_kpis Glue job."
  type        = string
}

variable "ingest_role_arn" {
  description = "IAM role ARN for the ingest_to_dynamodb Glue job."
  type        = string
}

variable "table_name" {
  description = "DynamoDB KPI table name (passed to the ingest job as --table_name)."
  type        = string
}

variable "utils_source_dir" {
  description = "Absolute path to src/utils/ — packaged into utils.zip by data.archive_file."
  type        = string
}

variable "glue_scripts_dir" {
  description = "Absolute path to src/glue_jobs/ — each .py file is uploaded to glue-assets/scripts/."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention for Glue job log groups."
  type        = number
  default     = 14
}

variable "transform_worker_type" {
  description = "Glue worker type for the PySpark transform job."
  type        = string
  default     = "G.1X"
}

variable "transform_number_of_workers" {
  description = "Number of workers for the PySpark transform job."
  type        = number
  default     = 2
}

variable "tags" {
  description = "Additional tags applied to every Glue resource in this module."
  type        = map(string)
  default     = {}
}
