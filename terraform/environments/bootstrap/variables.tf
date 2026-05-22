variable "region" {
  description = "AWS region — only used so the AWS provider has somewhere to talk to. IAM is global; resources land in any region."
  type        = string
  default     = "eu-central-1"
}

variable "github_org" {
  description = "GitHub organisation or user owning the repository."
  type        = string
}

variable "github_repo" {
  description = "GitHub repository name."
  type        = string
}

variable "create_oidc_provider" {
  description = "Set to false if a GitHub OIDC provider already exists in this account."
  type        = bool
  default     = true
}

variable "project_name" {
  description = "Short project identifier."
  type        = string
  default     = "music-streaming-pipeline"
}
