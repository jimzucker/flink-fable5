data "confluent_organization" "main" {}

resource "confluent_environment" "main" {
  display_name = var.project
}

# Basic cluster: no base charge, pay-per-use — right-sized for a demo that
# tears down same-day.
resource "confluent_kafka_cluster" "main" {
  display_name = "${var.project}-kafka"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.region

  basic {}

  environment {
    id = confluent_environment.main.id
  }
}

# One service account owns everything — demo-grade; production would split
# statement-runner, producer, and admin principals.
resource "confluent_service_account" "app" {
  display_name = "${var.project}-app"
  description  = "Runs Flink statements and owns the Kafka/Flink API keys"
}

resource "confluent_role_binding" "app_env_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

# Kafka API key — used by the local generator and the validation script.
resource "confluent_api_key" "kafka" {
  display_name = "${var.project}-kafka-key"
  description  = "Generator + validation access"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.app_env_admin]
}

data "confluent_flink_region" "main" {
  cloud  = "AWS"
  region = var.region
}

resource "confluent_flink_compute_pool" "main" {
  display_name = "${var.project}-pool"
  cloud        = "AWS"
  region       = var.region
  max_cfu      = var.max_cfu

  environment {
    id = confluent_environment.main.id
  }
}

# Flink API key — used by Terraform itself to submit the SQL statements.
resource "confluent_api_key" "flink" {
  display_name = "${var.project}-flink-key"
  description  = "Statement submission"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.app_env_admin]
}
