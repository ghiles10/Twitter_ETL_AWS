output "region" {
  description = "Region AWS"
  value       = var.aws_region
}

output "redshift_dns_name" {
  description = "Redshift DNS name."
  value       = aws_redshift_cluster.dwh_cluster.dns_name
}



