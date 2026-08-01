# AWS Deployment Runbook

Same jar as the laptop; every difference is configuration. Stack: **MSK
Serverless** (Kafka, IAM auth) + **Amazon Managed Service for Apache Flink**
(the pipeline, Flink 1.20 runtime) + **ECS Fargate** (the generator) +
**CloudWatch dashboard** (all Flink metrics incl. the custom `demo*` ones).
Managed Grafana can be pointed at the same CloudWatch namespace if preferred —
the dashboard here is IaC-provisioned and needs no SSO setup.

## Prerequisites
- AWS credentials configured (`aws sts get-caller-identity` works)
- Terraform >= 1.5, Docker, Maven, JDK 17+

## Deploy from scratch

```bash
make build                          # 1. shaded jar (also used by the image)
cd infra
terraform init
terraform apply                     # 2. VPC, MSK, MSF app, ECS, dashboard (~5-10 min; MSK takes a while)

# 3. generator image (ECR repo now exists)
make -C .. aws-push-generator REGION=$(terraform output -raw msk_bootstrap_brokers_iam >/dev/null; terraform output -json | jq -r '.generator_ecr_repo.value | split("/")[0] | split(".")[3]')
# or simply: make aws-push-generator REGION=us-east-1

# 4. restart the service so it picks the image
aws ecs update-service --cluster flink-demo --service flink-demo-generator --force-new-deployment

# 5. watch
terraform output cloudwatch_dashboard_url
```

The generator creates all six topics on startup (`--topics.partitions 4`,
idempotent), so no manual topic setup.

## Update the job (new code)

```bash
make build
cd infra && terraform apply         # jar hash changed -> new S3 version -> MSF updates in place
```

## Rescale (config only — the requirement)

```bash
terraform apply -var flink_parallelism=4        # no rebuild, no code change
```

## Performance validation cases (Phase 6)

```bash
# Case 1: 1000 orders/sec
terraform apply -var generator_trades_per_sec=1000

# Case 2: absurd price, same order rate
terraform apply -var generator_trades_per_sec=1000 -var generator_price_cents_override=1000000000000000
```

Watch on the dashboard: throughput stays flat, busy/backpressure time explains
any latency growth; Case 2 must not move the order-path metrics.

## Reading the numbers

Same metrics as local Grafana, namespace `AWS/KinesisAnalytics`, dimensions
`Application` + `Operator` (custom metrics appear at OPERATOR monitoring
level): `numRecordsOutPerSecond`, `demoBytesInPerSecond`,
`user_demoBytesOutPerSecond`, `demoDuplicatesDropped`, `demoMalformed`,
`busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond`.

## Teardown

```bash
cd infra && terraform destroy       # force_destroy on bucket/ECR handles contents
```

## Cost note (rough, us-east-1)

MSF ~2 KPU ≈ $0.22/hr; MSK Serverless ≈ $0.75/hr cluster + usage; NAT ≈
$0.045/hr; Fargate 0.25 vCPU ≈ $0.01/hr. **≈ $1/hr while running — destroy
when not demoing.**
