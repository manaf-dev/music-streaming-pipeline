output "oidc_provider_arn" {
  description = "ARN of the GitHub OIDC provider."
  value       = module.github_oidc.oidc_provider_arn
}

output "dev_role_arn" {
  description = "Dev deploy role ARN."
  value       = module.github_oidc.dev_role_arn
}

output "dev_role_name" {
  description = "Dev deploy role name — set AWS_ROLE_NAME_DEV to this value."
  value       = module.github_oidc.dev_role_name
}

output "prod_role_arn" {
  description = "Prod deploy role ARN."
  value       = module.github_oidc.prod_role_arn
}

output "prod_role_name" {
  description = "Prod deploy role name — set AWS_ROLE_NAME_PROD to this value."
  value       = module.github_oidc.prod_role_name
}
