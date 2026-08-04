# Confluent Cloud Runbook (Phase 9)

The same trading pipeline, re-implemented in **Flink SQL on Confluent Cloud**.
The AWS/DataStream version (see AWS_RUNBOOK.md) is unchanged — this is a
second deployment target, not a replacement. Why it's a rewrite and not a
port: Confluent Cloud's managed Flink exposes Flink SQL and the Table API
only, no DataStream API (full analysis: prompts/phase9_prompt.txt).

## Architecture

```
Java generator (local laptop, unchanged jar)
      │  plain JSON, SASL_SSL PLAIN
      ▼
Confluent Cloud Basic cluster (topics: trades, prices)
      ▼
Flink compute pool — 6 SQL statements (confluent/sql/dml/):
  10  trades ──dedup (ROW_NUMBER per trade_id)──▶ trades-dedup
  11  trades-dedup ──SUM──▶ position-by-account-ticker
  12  trades-dedup ──SUM──▶ position-by-ticker
  13  prices ──250ms tumbling window, last per symbol──▶ prices-conflated   ← the Phase 7 fix, as SQL
  14  positions ⋈ latest conflated price ──▶ mv-by-account-ticker
  15  positions ⋈ latest conflated price ──▶ mv-by-ticker
```

Design decision: **all topics stay plain JSON with the same field names as
the Java pipeline** (raw-format tables + `JSON_VALUE`/`JSON_OBJECT`, no
Schema Registry). That's what lets the unchanged Java generator and the same
five independent validation checks run against both stacks.

## Prerequisites

1. A Confluent Cloud account (new accounts get free credits, more than enough).
2. A **Cloud API key** (Settings → API keys → "Cloud resource management"
   scope, OrganizationAdmin): `export CONFLUENT_CLOUD_API_KEY=... CONFLUENT_CLOUD_API_SECRET=...`
3. Terraform ≥ 1.5, Java 11+, `pip install confluent-kafka` (validation only).

## Deploy

```bash
cd infra-confluent
terraform init
terraform apply        # ~3-5 min: env, Basic cluster, pool, 8 DDL + 6 DML statements
```

`apply` also writes `config/confluent.properties` (gitignored — contains the
Kafka API secret) for the generator and validation script.

Start data flowing (local, no ECS needed):

```bash
scripts/confluent_generator.sh --generator.trades.per.sec 10 --generator.prices.per.sec 20
```

Watch it: Confluent Cloud console → Environments → flink-fable5 → Flink →
statements (per-statement metrics), or `scripts/confluent_perf_probe.py`.

## Validate

Stop the generator (Ctrl-C), wait ~15 s, then:

```bash
python3 scripts/confluent_validate.py
```

Same five checks as local/AWS: dedup, positions reproducible (both keys),
completeness invariant, MV = position × final price (exact decimals).

## Load test

The scaling dial here is `max_cfu` (infra-confluent/variables.tf) — the
compute pool autoscales CFUs up to that cap per statement demand. There is no
explicit `flink_parallelism`; that's the trade for serverless.

```bash
scripts/confluent_generator.sh --generator.trades.per.sec 1000 --generator.prices.per.sec 10000 --generator.accounts 50
python3 scripts/confluent_perf_probe.py --minutes 10
```

Note: generator runs from the laptop, so the input rate is also bounded by
your uplink (~2-5k msgs/s of JSON is realistic on home broadband; the AWS
runs used an in-VPC Fargate generator for the 110k msgs/s numbers — compare
like with like).

## Teardown

```bash
cd infra-confluent
terraform destroy
```

Verify the zeros: console shows no environments; or
`confluent environment list` (CLI) returns empty; billing → no active
resources. Also delete `config/confluent.properties`.

## Gotchas (found while building — updated as deployment proceeds)

1. **No DataStream API on Confluent Cloud.** SQL/Table API only. The
   KeyedProcessFunction jar cannot run there; this directory is a semantic
   re-implementation, proven equivalent by the validation suite.
2. **Plain JSON needs raw-format tables.** Confluent's schema inference
   expects Schema Registry; schemaless JSON topics surface as raw bytes.
   Tables here are declared `(key STRING, val STRING)` with
   `'value.format'='raw'` and parsed with `JSON_VALUE` — byte-compatible
   with the Java stack.
3. **Quiesce flush differs from the Java timer.** The DataStream conflation
   timer fires 250 ms after the last tick regardless; a SQL tumbling window
   closes only when the watermark passes it, so the final price tick can sit
   in an unclosed window after the generator stops. The validation script
   uses the final *conflated* price for the MV checks and WARNs on staleness
   instead of failing.
4. **Statement state is retained, not TTL'd per operator.** Dedup state
   lifetime is governed by statement-level state retention, not the explicit
   per-operator TTL the Java version sets.
5. **Custom metrics don't exist.** No demoDuplicatesDropped counters — use
   per-statement metrics in the console plus the Metrics API
   (`scripts/confluent_perf_probe.py`).
6. **Statement-name 409 race on first apply.** With a freshly minted Flink
   API key, the provider's first submit can land slowly, its retry then hits
   `409 Statement already exists`, and the statements are left FAILED and
   outside Terraform state (the Confluent twin of the AWS MSF
   tag-propagation race from Phase 8). Fix: delete the orphaned statements
   via the Flink REST API and re-apply.
7. **Statements ≠ tables.** Deleting a statement record does NOT drop the
   table/topic it created, so a re-run of plain `CREATE TABLE` dies on
   `table already exists`. All DDL here is `CREATE TABLE IF NOT EXISTS` —
   which is what you want in IaC anyway.
8. **JDK 25 silently breaks the Java Kafka client.** The kafka-clients
   bundled with Flink 1.20 use JAAS APIs removed in modern JDKs: the SASL
   channel fails to construct, the producer retries metadata forever, and
   with no slf4j binding on the classpath *nothing is logged* — the
   generator ran 2.5 min printing its banner while delivering zero records.
   Diagnosed with a one-off `send().get()` probe; fixed by pinning
   Java 17 (`confluent_generator.sh` does this automatically).
9. **"Drained" means minutes, not seconds.** Exactly-once sinks publish
   results only at checkpoint commits. Right after stopping the generator,
   outputs are stale but internally consistent (positions ↔ MV agree with
   each other, both behind the raw topics) and converge to exact over the
   next checkpoint cycles. Wait ~3-5 min before `confluent_validate.py`.
10. **`max_cfu` is an enum and one-way.** Only {5, 10, 20, 30, 40, 50} are
    accepted, and it can **never be lowered** on an existing pool. For
    scaling experiments, create a *new pool per rung* — statements pick
    their pool, and pools bill only actual CFU-minutes, so idle extra pools
    are free. Destroying the environment cascades pool deletion.
11. **Statement names must be lowercase** `[a-z0-9-]`; anything else returns
    a cryptic "Request is malformed. name" 400.
12. **Throughput is bound by key cardinality, not CFUs.** A single statement
    hit ~265k/sec on 10 CFUs and only ~287k on 16 — but jumped to 386k when
    the symbol count went 10 → 30. Window/aggregation operators parallelize
    across *keys*. Corollary: sharding a narrow key space across two pools
    made things **worse** (235k combined vs 302k on one), because each shard
    got half the keys.
13. **Latency is seconds, by architecture.** Each statement hands off through
    Kafka — a write, a read, and a checkpoint commit per hop. Measured
    end-to-end through a 3-statement chain: p50 33 s, p99 44 s, versus
    267 ms / 495 ms for the equivalent single DataStream job. Not a tuning
    problem; it is what independent statements cost. Budget accordingly.
