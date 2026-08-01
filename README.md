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

## Current pipeline (Phase 2 skeleton)

```
trades (Kafka) -> parse -> dedup(trade_id, TTL state) -> keyBy(account,ticker) running sum -> position-by-account-ticker (Kafka, keyed upsert stream)
```

The generator also produces the `prices` topic; market-value operators land in Phase 3.
