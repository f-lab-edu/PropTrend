output "region" {
  value = var.region
}

output "state_bucket" {
  description = "infra/main의 terraform init -backend-config=\"bucket=...\"에 넣는다."
  value       = aws_s3_bucket.state.bucket
}

output "api_repository_url" {
  value = aws_ecr_repository.this["api"].repository_url
}

output "airflow_repository_url" {
  value = aws_ecr_repository.this["airflow"].repository_url
}
