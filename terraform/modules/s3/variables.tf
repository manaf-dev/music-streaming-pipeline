variable "bucket_name" {
  description = "Globally unique S3 bucket name. Should include account id or random suffix to avoid collisions."
  type        = string
}

variable "project_name" {
  description = "Short project identifier used in tags and resource names."
  type        = string
  default     = "music-streaming-pipeline"
}

variable "data_dir" {
  description = "Absolute or module-relative path to the local data/ directory containing songs.csv and users.csv."
  type        = string
}

variable "raw_expiration_days" {
  description = "Days after which objects under raw/ are deleted by lifecycle policy."
  type        = number
  default     = 90
}

variable "archive_transition_days" {
  description = "Days after which objects under archive/ transition to STANDARD_IA."
  type        = number
  default     = 90
}

variable "processed_expiration_days" {
  description = "Days after which intermediate Parquet partials under processed/ are deleted. These are per-execution handoff artifacts that are no longer needed once ingested into DynamoDB."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Additional tags applied to all resources in this module."
  type        = map(string)
  default     = {}
}
