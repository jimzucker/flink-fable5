output "msk_bootstrap_brokers_iam" {
  value = data.aws_msk_bootstrap_brokers.this.bootstrap_brokers_sasl_iam
}

output "artifacts_bucket" {
  value = aws_s3_bucket.artifacts.id
}

output "flink_application" {
  value = aws_kinesisanalyticsv2_application.this.name
}

output "generator_ecr_repo" {
  value = aws_ecr_repository.generator.repository_url
}

output "cloudwatch_dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards/dashboard/${var.project}"
}
