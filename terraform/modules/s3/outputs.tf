output "bucket_id" {
  description = "Bucket id (same value as bucket_name)."
  value       = aws_s3_bucket.main.id
}

output "bucket_arn" {
  description = "ARN of the bucket — pass to the IAM module for least-privilege policies."
  value       = aws_s3_bucket.main.arn
}

output "bucket_name" {
  description = "Bucket name — useful for variable substitution in ASL and scripts."
  value       = aws_s3_bucket.main.bucket
}

output "bucket_regional_domain_name" {
  description = "Regional domain name (e.g. for S3 transfer-acceleration or direct access)."
  value       = aws_s3_bucket.main.bucket_regional_domain_name
}
