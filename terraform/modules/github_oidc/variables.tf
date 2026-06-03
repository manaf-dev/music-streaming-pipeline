variable "github_org" {
  description = "GitHub organisation or user owning the repository (e.g. \"AmaliTech\")."
  type        = string
}

variable "github_repo" {
  description = "GitHub repository name (e.g. \"music-streaming-pipeline\")."
  type        = string
}

variable "project_name" {
  description = "Short project identifier used in role names and tags."
  type        = string
  default     = "music-streaming-pipeline"
}

variable "create_oidc_provider" {
  description = "Set to false if your account already has a GitHub OIDC provider; the roles will reference it via data source instead of creating a duplicate."
  type        = bool
  default     = true
}

variable "dev_branch" {
  description = "Branch authorised to assume the dev role (push trigger for CD dev)."
  type        = string
  default     = "development"
}

variable "prod_branch" {
  description = "Branch authorised to assume the prod role (push trigger for CD prod)."
  type        = string
  default     = "main"
}

variable "dev_environment_name" {
  description = "GitHub Actions environment name used by the dev workflow (matches `environment:` in cd-dev.yml)."
  type        = string
  default     = "development"
}

variable "prod_environment_name" {
  description = "GitHub Actions environment name used by the prod workflow (matches `environment:` in cd-prod.yml)."
  type        = string
  default     = "production"
}

variable "tags" {
  description = "Additional tags applied to every IAM resource in this module."
  type        = map(string)
  default     = {}
}
