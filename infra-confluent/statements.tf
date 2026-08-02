# The pipeline itself: every .sql file under confluent/sql becomes a Flink
# statement. DDL (CREATE TABLE → backing topics) runs before DML (the
# long-running INSERT jobs). File content changes force replacement — edit a
# .sql file and `terraform apply` redeploys that statement.
locals {
  sql_root  = "${path.module}/../confluent/sql"
  ddl_files = sort(fileset("${local.sql_root}/ddl", "*.sql"))
  dml_files = sort(fileset("${local.sql_root}/dml", "*.sql"))

  statement_properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }
}

resource "confluent_flink_statement" "ddl" {
  for_each = toset(local.ddl_files)

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement      = file("${local.sql_root}/ddl/${each.value}")
  statement_name = "ddl-${replace(replace(each.value, ".sql", ""), "_", "-")}"
  properties     = local.statement_properties

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_role_binding.app_env_admin,
    confluent_kafka_cluster.main,
  ]
}

resource "confluent_flink_statement" "dml" {
  for_each = toset(local.dml_files)

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement      = file("${local.sql_root}/dml/${each.value}")
  statement_name = "dml-${replace(replace(each.value, ".sql", ""), "_", "-")}"
  properties     = local.statement_properties

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [confluent_flink_statement.ddl]
}
