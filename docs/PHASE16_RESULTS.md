# Phase 16 — measured results

Every number below was produced in one session on matched rigs (48 partitions,
exactly-once, salt factor 8, fused single job) and is either **output-verified**
or explicitly marked withdrawn. Numbers inherited from earlier phases are
retired rather than carried forward, because several were measured against
ceilings built into the test rig.

## Throughput

| Cell | Consumed / window | Throughput | Output verified |
|---|---|---|---|
| **DataStream (AWS), salted** | 143.68M / 982s | **~146,300 rec/s** | structurally immune |
| SQL (Confluent), cap-20 | 63.74M / 807s | ~79,000 rec/s | yes |
| SQL (Confluent), cap-10 | 36.68M / 695s | ~52,800 rec/s | yes |
| DataStream (AWS), unsalted *(control)* | ~140M / 2613s | ~53,600 rec/s | structurally immune |
| ~~SQL (AWS)~~ | ~144M / 2646s | ~~~54,400 rec/s~~ | **NO — withdrawn** |

AWS: parallelism 20, ~6 billed KPU, ~$1.09/hr including MSK partitions.
Confluent: Basic/eCKU cluster, compute billed per CFU-minute only while running.

## The five findings

### 1. Salting is worth 2.66x on DataStream — and I predicted the opposite

Salted 982s vs unsalted 2613s, same seed procedure, one variable changed.

I argued salting would be *net overhead* on DataStream because it adds a keyed
shuffle plus per-key state and timers, and because the job "did not have
Confluent's key-starvation problem". It has exactly that problem: four of six
`keyBy` stages sit on ten tickers and the whole price feed funnels through them.
Conflating *before* the narrowing is the entire game, and it is worth more here
(+176%) than on Confluent (+85%). **Test a tuning on each platform; do not
reason about whether a win transfers.**

### 2. Confluent scales 1.50x for 2x CFUs — the old verdict was our own ceiling

| Rung | Rate |
|---|---|
| cap-10 | 52,779 rec/s |
| cap-20 | 78,980 rec/s |

This **replaces** the earlier "10 → 16 CFUs = +8%, the CFU dial saturates"
finding. That was measured against **6-bucket tables**, and a Flink source
cannot use more subtasks than the topic has partitions — so added CFUs had
nothing to read. Confluent was never given the chance to scale.

The 25% shortfall from linear is a **workload** ceiling, not a vendor one: the
salted conflation parallelises across 80 shard keys and does scale, while the
downstream market-value stages stay pinned at ten tickers. Any Flink deployment
anywhere would hit this with a ten-key workload.

Backlogs differed between rungs, so **rates were compared, not drain times** —
cap-20 took *longer* in wall-clock (807s vs 695s) while being substantially
faster per second. Comparing times would have inverted the conclusion.

### 3. Idle partitions stall event-time watermarks — self-inflicted, and it cost a result

Ten tickers hashed into 48 buckets leaves **38 buckets permanently empty**. An
empty partition never advances its watermark, so the event-time `TUMBLE` never
fires. Symptom: statement `RUNNING`, consuming 119k/s, writing **zero rows**,
for an hour, with no error and no DEGRADED.

Raising buckets to remove the source-parallelism ceiling is what created this.
`partitions >= parallelism` is only half the rule; the other half is
`partitions <= key cardinality, or set an idle timeout`.

Fix verified: `sql.tables.scan.idle-timeout=5000` (OSS/AWS:
`table.exec.source.idle-timeout`). Output appeared on all four sinks at once.

**DataStream is structurally immune** — `noWatermarks()` and processing-time
timers throughout. That is a robustness argument for the DataStream formulation
independent of speed.

### 4. CR-1 is not expressible in Confluent SQL

AWS SQL implements the cadence cap with the upsert-kafka
`sink.buffer-flush.interval`. Confluent **rejects the option**; its own
supported-options list contains no sink buffering or cadence control at all.
The CUMULATE workaround was tried and starved the outputs (p50 158–235s).

A plain business requirement — *don't update a number faster than a human can
read it* — is one connector option on one platform and currently impossible on
the other. Report it as a **capability** row, never as a speed row.

### 5. Partitions: a cost knob on AWS, free on Confluent

The cluster is Basic on the eCKU model, which has **no per-partition charge**.
288 partitions (48 x 6 topics) cost **$0** there and **~$0.43/hr (~$315/mo)** on
MSK. On AWS, using your compute efficiently costs extra money — any TCO
comparison that prices compute but not the partitions needed to keep that
compute busy is understating AWS.

Related: Confluent **names** query-shape problems in its console
(`UPSERT_AND_PRIMARY_KEYS_DIFFERENT`) with a doc link and a warning that they
cost CFUs. MSF does the same expensive thing silently — the
`SinkUpsertMaterializer` was only found by reading an execution plan.

## What is still outstanding

* **Re-run SQL (AWS) with `table.exec.source.idle-timeout`.** Its output was
  never verified and the dashboard showed an empty sink panel, so ~54,400 rec/s
  may have measured reading rather than processing. Until then there is **no
  sound DataStream-vs-SQL ratio** and no cross-cloud SQL claim.
* **Trade/price mix differs slightly** between clouds (1.0% vs 0.42% trades)
  because in-cloud amplification copies only `prices`. Prices are the expensive
  path, so this mildly flatters Confluent.
* **README, deck and article** still carry retired numbers (435k/s AWS; "+8%,
  Confluent saturates"). They need one coherent rewrite, not per-cell patches.

## Method notes that changed results

* **Size the backlog against the drain rate**, not the clock:
  `seed > rate x (blind_spot + plateau)`. Two seeds were discarded for being
  drained in seconds.
* **Verify the output side.** Every cell was validated on inputs — consumed,
  backpressure, restarts, checkpoints — and none on outputs. A pipeline that
  reads fast and writes nothing scores beautifully.
* **Health-gate before measuring.** One full drain returned plausible zeros from
  a crash-looping job (`RUNNING`, 100% busy, no backpressure, no FAILED).
  Preserved as `docs/evidence/phase16/02_drain_all_zeros_crashloop.log`.
* **`transaction.timeout.ms` is a correctness requirement of exactly-once**, not
  tuning: Flink defaults to 1 hour, MSK caps at 15 minutes, and the mismatch
  restart-loops silently. Now a terraform variable default.
