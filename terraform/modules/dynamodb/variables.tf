variable "env" {
  description = "Deployment environment (dev, prod)."
  type        = string
}

variable "project_name" {
  description = "Short project identifier used in tags and resource names."
  type        = string
  default     = "music-streaming-pipeline"
}

variable "table_name" {
  description = "DynamoDB table name. Defaults to <env>-music-streaming-kpis when null."
  type        = string
  default     = null
}

variable "ttl_attribute" {
  description = "Attribute name carrying epoch-second expiration timestamps. Items past TTL are auto-deleted by DynamoDB."
  type        = string
  default     = "expires_at"
}

variable "deletion_protection_enabled" {
  description = "When true, deletion of the table requires manual disable first. Recommended for prod."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags applied to the table."
  type        = map(string)
  default     = {}
}
