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

## Deployment gotchas (learned the hard way, 2026-08-01)

1. **Build for Java 11.** MSF's FLINK-1_20 runtime is Java 11; Java 17
   bytecode fails with "entry point class could not be loaded due to a
   linkage failure." `maven.compiler.release` is 11 in the pom — and if you
   change it, **`mvn clean package`**: an incremental build keeps stale
   classes when only the pom changed. Verify before uploading:
   `unzip -p target/flink-demo.jar <cls>.class | head -c 8 | od -A n -t u1`
   → 8th byte 55 = Java 11.
2. **Build the generator image for linux/amd64.** On Apple Silicon:
   `docker build --platform linux/amd64 ...` or Fargate fails with
   `CannotPullContainerError ... does not contain descriptor matching
   platform`.
3. **MSF destroy→recreate races AWS tag propagation.** If apply fails with
   `ConcurrentModificationException: Tags are already registered` but the
   app then exists, Terraform doesn't know it owns it:
   `terraform import aws_kinesisanalyticsv2_application.this <arn>` then
   re-apply.
4. **After a failed start the app returns to READY silently** — check
   `aws logs tail /aws/kinesis-analytics/flink-demo` for the real error;
   `start_application=true` in Terraform does not re-start an app that
   failed once.
5. zsh: `"$VAR:latest"` triggers the `:l` modifier — write `"${VAR}:latest"`.
6. **MSF silently drops plain custom metrics.** User metrics reach CloudWatch
   only when registered under a `kinesisanalytics` metric group
   (`group.addGroup("kinesisanalytics").counter(...)`); they then appear with
   `Application + Task` dimensions. This repo dual-registers via
   `DemoMetrics` so Prometheus (local) and CloudWatch (AWS) both see them.
7. **Dashboard dimensioning:** built-in operator metrics use
   `Task`/`TaskOperator` dimensions (not `Operator`); custom metrics land at
   `Task` level. CloudWatch metrics are 1-min granularity — expect ~7-8 min
   per load-test iteration vs seconds with local Prometheus.

## Cost note (rough, us-east-1)

MSF ~2 KPU ≈ $0.22/hr; MSK Serverless ≈ $0.75/hr cluster + usage; NAT ≈
$0.045/hr; Fargate 0.25 vCPU ≈ $0.01/hr. **≈ $1/hr while running — destroy
when not demoing.**
