output "bootstrap" {
  description = "Kafka bootstrap endpoint (host:port, SASL_SSL)"
  value       = replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")
}

output "kafka_api_key" {
  value     = confluent_api_key.kafka.id
  sensitive = true
}

output "kafka_api_secret" {
  value     = confluent_api_key.kafka.secret
  sensitive = true
}

output "environment_id" {
  value = confluent_environment.main.id
}

output "kafka_cluster_id" {
  description = "lkc-… id, needed by the Metrics API perf probe"
  value       = confluent_kafka_cluster.main.id
}

output "compute_pool_id" {
  value = confluent_flink_compute_pool.main.id
}

output "flink_rest_endpoint" {
  value = data.confluent_flink_region.main.rest_endpoint
}

# Client config for the local generator and validation script — written
# straight to the repo's config/ dir (gitignored: contains the API secret).
resource "local_sensitive_file" "client_properties" {
  filename        = "${path.module}/../config/confluent.properties"
  file_permission = "0600"

  content = <<-EOT
    bootstrap.servers=${replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")}
    security.protocol=SASL_SSL
    sasl.mechanism=PLAIN
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='${confluent_api_key.kafka.id}' password='${confluent_api_key.kafka.secret}';
  EOT
}
