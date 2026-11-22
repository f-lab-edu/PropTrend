output "region" {
  value = var.region
}

output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "api_target_group_arn" {
  description = "k8s/overlays/eks의 TargetGroupBinding에 eks.sh가 넣는다."
  value       = aws_lb_target_group.api.arn
}

output "app_url" {
  value = "https://${aws_cloudfront_distribution.api.domain_name}"
}

output "rds_endpoint" {
  value = aws_db_instance.this.address
}
