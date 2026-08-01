resource "aws_security_group" "msk" {
  name_prefix = "${var.project}-msk-"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "Kafka IAM (9098) from Flink app and generator"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.flink.id, aws_security_group.generator.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = var.project

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

data "aws_msk_bootstrap_brokers" "this" {
  cluster_arn = aws_msk_serverless_cluster.this.arn
}

locals {
  # MSK IAM auth client properties, passed through AppConfig's kafka.props.* mechanism
  msk_iam_props = {
    "kafka.props.security.protocol"                  = "SASL_SSL"
    "kafka.props.sasl.mechanism"                     = "AWS_MSK_IAM"
    "kafka.props.sasl.jaas.config"                   = "software.amazon.msk.auth.iam.IAMLoginModule required;"
    "kafka.props.sasl.client.callback.handler.class" = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
  }
}
