output "validate_job_name" {
  description = "Name of the validate_schema Glue job."
  value       = aws_glue_job.validate.name
}

output "transform_job_name" {
  description = "Name of the transform_kpis Glue job."
  value       = aws_glue_job.transform.name
}

output "ingest_job_name" {
  description = "Name of the ingest_to_dynamodb Glue job."
  value       = aws_glue_job.ingest.name
}

output "archive_job_name" {
  description = "Name of the archive_files Glue job."
  value       = aws_glue_job.archive.name
}

output "job_arns" {
  description = "ARNs of every Glue job — handy for IAM policy verification."
  value = {
    validate  = aws_glue_job.validate.arn
    transform = aws_glue_job.transform.arn
    ingest    = aws_glue_job.ingest.arn
    archive   = aws_glue_job.archive.arn
  }
}

output "utils_zip_s3_key" {
  description = "S3 key of the packaged utils.zip."
  value       = aws_s3_object.utils_zip.key
}

output "log_group_names" {
  description = "CloudWatch log group names keyed by job short name."
  value       = { for k, lg in aws_cloudwatch_log_group.jobs : k => lg.name }
}
