# flink-demo

Deterministic Flink streaming demo: mocked block trades and ticking prices in Kafka,
real-time positions and market values out. Runs on a laptop with Docker Compose,
deploys to AWS with the same jar. See [PLAN.md](PLAN.md) for the design and phase plan.

## Quick start (laptop)

Prereqs: JDK 17+, Maven, Docker Desktop.

```bash
make up          # builds the jar, starts Kafka + Flink + generator + Prometheus + Grafana
make positions   # tail the position-by-account-ticker output topic
make status      # Flink job status via REST
```

UIs:
- Flink dashboard: http://localhost:8081
- Grafana: http://localhost:3000 (anonymous admin enabled for demo)
- Prometheus: http://localhost:9090

Stop everything: `make down` (add `make clean` to also wipe state).

## Changing behavior — no rebuild needed

Edit [config/application.properties](config/application.properties) and restart the
affected container (`docker compose restart generator`, or resubmit the job). Knobs:
generator rates, universe sizes, seed, duplicate ratio, price override (perf Case 2),
pipeline parallelism, checkpoint interval, dedup TTL.

## Current pipeline (Phase 3)

```
trades -> parse -> dedup(trade_id, TTL state) -+-> position by account+ticker -+-> Kafka
                                               |                               +-> (x latest price) mv by account+ticker -> Kafka
                                               +-> position by ticker         -+-> Kafka
                                                                               +-> (x latest price) mv by ticker -> Kafka
prices -> parse (exact long cents) ----------------> latest price per ticker (keyed co-process joins)
```

All money math is exact (long cents / BigDecimal) — no floating point, any price magnitude.

## Observability

Grafana auto-provisions the **"Flink Demo — Pipeline Observability"** dashboard
(http://localhost:3000/d/flink-demo): records/sec and totals per operator, volume
in/out in bytes/sec (KB/s) per parser and sink, duplicates dropped, malformed
records, checkpoint duration/size, busy/backpressure time per task, Kafka lag.
Custom metrics carry the `demo` prefix (`demoBytesInPerSecond`,
`user_demoBytesOutPerSecond`, `demoDuplicatesDropped`, ...).

Tail any output topic: `make tail TOPIC=mv-by-ticker`

## AWS

`infra/` holds the full Terraform stack (MSK Serverless + Managed Service for
Apache Flink + Fargate generator + CloudWatch dashboard) — same jar, config-only
differences, rescale with `terraform apply -var flink_parallelism=N`.
See [docs/AWS_RUNBOOK.md](docs/AWS_RUNBOOK.md).

## Correctness

- `make test` — 14 JUnit tests: operator harnesses + a hand-computed golden
  end-to-end dataset (see [VALIDATION.md](VALIDATION.md))
- `make validate` — recomputes all outputs independently from the raw Kafka
  topics on the running stack and checks dedup, reproducibility, completeness
  (Σ accounts == ticker) and exact market values
