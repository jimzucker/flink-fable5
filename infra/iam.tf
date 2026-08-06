data "aws_caller_identity" "current" {}

locals {
  msk_cluster_arn = aws_msk_serverless_cluster.this.arn
  # topic/group ARNs share the cluster's name/uuid path
  msk_arn_parts = split("/", local.msk_cluster_arn)
  msk_path      = "${local.msk_arn_parts[1]}/${local.msk_arn_parts[2]}"
  msk_base      = "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}"

  msk_access_statement = {
    Effect = "Allow"
    Action = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DeleteTopic",
      "kafka-cluster:AlterTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
      # Exactly-once sinks use transactional producers. Without these three
      # (and the transactional-id resource below) the producer cannot
      # initialize its transaction, the sink fails, and the job crash-loops
      # at ANY parallelism and ANY transaction timeout — with no useful
      # error surfaced in the MSF application logs.
      "kafka-cluster:WriteDataIdempotently",
      "kafka-cluster:AlterTransactionalId",
      "kafka-cluster:DescribeTransactionalId"
    ]
    Resource = [
      local.msk_cluster_arn,
      "${local.msk_base}:topic/${local.msk_path}/*",
      "${local.msk_base}:group/${local.msk_path}/*",
      "${local.msk_base}:transactional-id/${local.msk_path}/*"
    ]
  }
}

# --- Managed Flink application role ---
resource "aws_iam_role" "flink" {
  name = "${var.project}-flink"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kinesisanalytics.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "flink" {
  name = "app"
  role = aws_iam_role.flink.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.msk_access_statement,
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.artifacts.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        # VPC-attached app manages its own ENIs
        Effect = "Allow"
        Action = [
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeDhcpOptions",
          "ec2:CreateNetworkInterface",
          "ec2:CreateNetworkInterfacePermission",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface"
        ]
        Resource = "*"
      }
    ]
  })
}

# --- Generator (ECS task) roles ---
resource "aws_iam_role" "generator_task" {
  name = "${var.project}-generator-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "generator_task" {
  name = "msk"
  role = aws_iam_role.generator_task.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [local.msk_access_statement]
  })
}

resource "aws_iam_role" "generator_execution" {
  name = "${var.project}-generator-exec"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "generator_execution" {
  role       = aws_iam_role.generator_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
