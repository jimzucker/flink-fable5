resource "aws_security_group" "flink" {
  name_prefix = "${var.project}-flink-"
  vpc_id      = aws_vpc.this.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "${var.project}-artifacts-"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "jar" {
  bucket      = aws_s3_bucket.artifacts.id
  key         = "flink-demo.jar"
  source      = var.jar_path
  source_hash = filemd5(var.jar_path)
  depends_on  = [aws_s3_bucket_versioning.artifacts]
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.project}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "app"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_kinesisanalyticsv2_application" "this" {
  name                   = var.project
  runtime_environment    = "FLINK-1_20"
  service_execution_role = aws_iam_role.flink.arn
  start_application      = true

  application_configuration {
    application_snapshot_configuration {
      # Keep on for production/rescale (without it a restart replays from
      # earliest). Turn OFF while debugging: MSF refuses to update an
      # application it cannot snapshot, and it cannot snapshot a crash-looping
      # one — so with snapshots on, a broken app cannot be fixed in place.
      snapshots_enabled = var.snapshots_enabled
    }

    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn     = aws_s3_bucket.artifacts.arn
          file_key       = aws_s3_object.jar.key
          object_version = aws_s3_object.jar.version_id
        }
      }
      code_content_type = "ZIPFILE"
    }

    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = merge(
          {
            "kafka.bootstrap.servers" = data.aws_msk_bootstrap_brokers.this.bootstrap_brokers_sasl_iam
            "checkpoint.interval.ms"  = "10000"
            "dedup.state.ttl.ms"      = "3600000"
          },
          local.msk_iam_props,
          { for k, v in var.kafka_extra_props : "kafka.props.${k}" => v },
          var.flink_extra_props
        )
      }
    }

    flink_application_configuration {
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.flink_parallelism
        parallelism_per_kpu  = var.flink_parallelism_per_kpu
        auto_scaling_enabled = false
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        # OPERATOR metrics stay: they cost ~$0.01/day and are what make
        # per-operator bottleneck profiling possible. LOG level is the expensive
        # dial -- see var.flink_log_level.
        metrics_level      = "OPERATOR"
        log_level          = var.flink_log_level
      }
      checkpoint_configuration {
        configuration_type = "DEFAULT"
      }
    }

    vpc_configuration {
      subnet_ids         = aws_subnet.private[*].id
      security_group_ids = [aws_security_group.flink.id]
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }
}
