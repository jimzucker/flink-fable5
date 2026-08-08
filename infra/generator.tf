resource "aws_security_group" "generator" {
  name_prefix = "${var.project}-generator-"
  vpc_id      = aws_vpc.this.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecr_repository" "generator" {
  name         = "${var.project}-generator"
  force_delete = true
}

resource "aws_ecs_cluster" "this" {
  name = var.project
}

resource "aws_cloudwatch_log_group" "generator" {
  name              = "/ecs/${var.project}-generator"
  retention_in_days = 7
}

locals {
  generator_args = concat(
    [
      "--kafka.bootstrap.servers", data.aws_msk_bootstrap_brokers.this.bootstrap_brokers_sasl_iam,
      "--generator.trades.per.sec", tostring(var.generator_trades_per_sec),
      "--generator.prices.per.sec", tostring(var.generator_prices_per_sec),
      "--generator.accounts", tostring(var.generator_accounts),
      "--generator.tickers", tostring(var.generator_tickers),
      # Realistic market shape: Pareto/Zipf across the universe plus one hot
      # listing. A uniform feed makes every symbol equally busy, which turns
      # "one worker per key" into apparent parallelism and hides the skew that
      # actually happens in production.
      "--generator.distribution", var.generator_distribution,
      "--generator.zipf.alpha", tostring(var.generator_zipf_alpha),
      "--generator.ipo.share", tostring(var.generator_ipo_share),
      "--generator.ipo.ticker", tostring(var.generator_ipo_ticker),
      "--generator.price.key.mode", var.generator_price_key_mode,
      "--generator.seed", tostring(var.generator_seed),
      "--generator.duplicate.ratio", tostring(var.generator_duplicate_ratio),
      "--generator.price.cents.override", tostring(var.generator_price_cents_override),
      # Correctness-run knobs. DataGenerator has always read these; terraform
      # never passed them, so the simple-numbers validation path was
      # unreachable on AWS and only ever ran locally.
      "--generator.qty.override", tostring(var.generator_qty_override),
      "--generator.price.per.symbol", tostring(var.generator_price_per_symbol),
      "--topics.partitions", tostring(var.topics_partitions),
      "--topics.recreate", tostring(var.topics_recreate),
    ],
    flatten([for k, v in local.msk_iam_props : ["--${k}", v]])
  )
}

resource "aws_ecs_task_definition" "generator" {
  family                   = "${var.project}-generator"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.generator_cpu
  memory                   = var.generator_memory
  execution_role_arn       = aws_iam_role.generator_execution.arn
  task_role_arn            = aws_iam_role.generator_task.arn

  container_definitions = jsonencode([{
    name      = "generator"
    image     = "${aws_ecr_repository.generator.repository_url}:latest"
    command   = local.generator_args
    essential = true
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.generator.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "generator"
      }
    }
  }])
}

resource "aws_ecs_service" "generator" {
  name            = "${var.project}-generator"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.generator.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.generator.id]
    assign_public_ip = false
  }
}
