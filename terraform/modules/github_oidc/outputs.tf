output "oidc_provider_arn" {
  description = "ARN of the GitHub OIDC provider (created or referenced)."
  value       = local.oidc_provider_arn
}

output "dev_role_arn" {
  description = "ARN of the dev deploy role — copy into the AWS_ROLE_NAME_DEV repo variable (name only) or use the ARN directly."
  value       = aws_iam_role.dev.arn
}

output "dev_role_name" {
  description = "Name of the dev deploy role — this is what AWS_ROLE_NAME_DEV should be set to."
  value       = aws_iam_role.dev.name
}

output "prod_role_arn" {
  description = "ARN of the prod deploy role."
  value       = aws_iam_role.prod.arn
}

output "prod_role_name" {
  description = "Name of the prod deploy role — set AWS_ROLE_NAME_PROD to this."
  value       = aws_iam_role.prod.name
}
