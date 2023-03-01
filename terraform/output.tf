output "region" {
  description = "Region AWS"
  value       = var.aws_region
}

output "bucket_name" {
  description = "S3 bucket."
  value       = aws_s3_bucket.bucket_name.id
}

output "redshift_dns_name" {
  description = "Redshift DNS name."
  value       = aws_redshift_cluster.dwh_cluster.dns_name
}



