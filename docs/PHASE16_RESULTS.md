# Results — measured, August 2026

> **⚠ SUPERSEDED — current results: [`PHASE19_RESULTS.md`](PHASE19_RESULTS.md).**
>
> Every throughput figure below was measured on a workload of **at most 30
> symbols**: a `Math.min(requested, 30)` clamp in the generator silently capped
> the ticker universe, and it was not found until Phase 18. The scaling
> conclusions in particular were properties of that benchmark, not of the
> platforms. The correctness posture here is also weaker than it should be —
> the validation suite never ran successfully during this phase.

**This file supersedes all earlier performance numbers in this repository.**
Every figure was measured in a single session on matched configurations and
verified to have produced output. Where the data cannot support a claim, the
claim is not made.

If you have seen the older numbers, read [What changed](#what-changed).

---

## The workload, first — because it explains everything else

| | |
|---|---|
| Tickers | **10** |
| Accounts | 5 → 50 `account\|ticker` keys |
| Trades | 100/s |
| Prices | **10,000/s** |
| Mix | **~99% price ticks** |

**Ten tickers is the defining constraint of this benchmark.** Flink assigns each
key to exactly one parallel worker, so a stage keyed on ticker can use at most
ten workers however much compute is attached. Four of six stages are keyed that
way, and the 99%-price mix routes almost all work through them.

Nearly every result below traces back to that one fact. A ten-key benchmark
measures key starvation at least as much as it measures a platform — read these
as results for *this workload on these platforms*, not as general verdicts.

---

## Throughput

Measured by backlog drain: pre-load the topics, start from earliest, divide
records consumed by elapsed time. Output was confirmed non-zero on all sinks
before any number was accepted.

| Configuration | Records / window | Throughput |
|---|---|---|
| **DataStream (AWS), salted** | 143.68M / 982s | **146,300 rec/s** |
| SQL (Confluent) | 4 runs | **39,400 – 90,600 rec/s** |
| SQL (AWS) | 84.11M / 1,093s | **76,956 rec/s** |
| DataStream (AWS), unsalted *(control)* | ~140M / 2,613s | 53,600 rec/s |

**Run-to-run variance is ~25%** — the same Confluent configuration measured
39,426 and 52,779 rec/s on separate runs. Treat any gap below ~25% as noise.

### What the data supports

**DataStream is roughly 2× faster than SQL here.** 146,300 vs 76,956 rec/s on
identical hardware, load, delivery guarantee and query semantics. Well outside
the noise band.

**SQL performs the same on both clouds.** AWS SQL's 76,956 rec/s sits inside
Confluent's measured range. No evidence of a platform difference in SQL
throughput — the language is the constraint, not the vendor.

**Conflating before the narrow stage is worth 2.66×.** Salted 982s vs unsalted
2,613s, one variable changed. The price feed funnels through ten ticker-keyed
workers; cutting volume *before* that narrowing is the largest single tuning win
in this project — bigger than any config knob.

---

## Scaling

**This workload saturates at ~10-way parallelism and cannot use more.**

A Confluent pool capped at 20 CFU was measured drawing **7.64 CFU average, 10
maximum** — it never touched half its cap, because ten tickers cap usable
parallelism at ten.

Two ceilings had to be removed before this could be measured honestly:

1. **Bucket count** — tables built with 6 buckets cap a source at 6 readers
   however many CFUs exist. Raised to 48.
2. **Key concentration** — the producer keys prices by symbol, so ten symbols
   occupy at most ten partitions whatever the topic width. Query-side salting
   cannot fix this: a downstream `PARTITION BY` runs *after* the source read.
   Fixing it needs salting at **write** time (parked on
   `parked/price-key-salting`, untested).

Removing the first revealed the second. **Scaling here is bounded by key
cardinality, not by either platform.**

---

## Cost

| | AWS | Confluent |
|---|---|---|
| Compute | $0.66/hr (6 billed KPU) | $1.60–2.02/hr (7.6–9.6 CFU measured) |
| Kafka base | $0.75/hr | eCKU — *not measured* |
| Partitions (288) | $0.43/hr | **free** |
| **Known total** | **$1.84/hr** | ≥$1.60/hr *(incomplete)* |

**64% of the AWS bill is Kafka, not Flink.** Every pipeline tuning change in
this project moved the smaller half of the invoice.

**Partitions are a cost knob on AWS and free on Confluent.** Reaching full
utilisation means buying partition-hours on MSK; the same change is free on
Confluent's eCKU model. Any TCO that prices compute but not the partitions
needed to keep compute busy understates AWS.

*Confluent's total is incomplete — its eCKU cluster charge was never captured,
so no cross-platform cost claim is made.*

---

## Where the platforms genuinely differ

Throughput is where they are most alike. These are the real differences.

### Output cadence: expressible on AWS, impossible on Confluent

A plain business requirement — *do not update a number faster than a human can
read it* (positions ≤1 update/key/500ms, market values ≤1/key/1000ms) — is one
connector option on AWS SQL (`sink.buffer-flush.interval`) and **cannot be
expressed in Confluent SQL at all.** Confluent rejects the option, and its
supported-options list contains no sink buffering or cadence control. The
CUMULATE workaround starves the outputs (p50 158–235s).

Verified on AWS DataStream under full saturation: 45.3 updates/s across 50 keys
against a 1/key/s cap.

### Diagnostics: Confluent tells you, MSF does not

Confluent names expensive query shapes in its console —
`UPSERT_AND_PRIMARY_KEYS_DIFFERENT`, `HIGH_STATE_OPERATOR_WITHOUT_TTL` — with
doc links and a warning that they cost CFUs. MSF runs the same
`SinkUpsertMaterializer` silently; it was found only by reading an execution
plan and spotting an operator moving 219k records/sec.

Both platforms had the same defect. Only one said so.

### Robustness: DataStream cannot fail the way SQL did

An event-time `TUMBLE` over partitions that never receive data never fires. With
ten tickers across 48 partitions, 38 sit permanently idle — and both SQL
implementations consumed at full speed while writing **zero rows**, reporting
`RUNNING` with no error. Fixed with a source idle timeout, now defaulted in both.

The DataStream job uses `noWatermarks()` and processing-time timers and is
structurally immune. That is an argument for it independent of speed.

### Metrics retention shapes how you can experiment

CloudWatch retains metrics ~15 months and stays queryable after teardown — the
entire AWS utilisation analysis was rebuilt post-destroy. Confluent telemetry
returns `403` once the pool is deleted. There, **teardown closes the measurement
window**, so utilisation must be captured during the run.

---

## Utilisation and waste

| Configuration | rec/s | CPU | Hot-stage slots |
|---|---|---|---|
| DataStream salted | 146,300 | 76.3% | 20/20 |
| SQL (AWS) | 76,956 | 66.5% | 20/20 |
| DataStream unsalted | 53,600 | 63.1% | **10/20** |

A **slot** is one parallel operator instance. Flink gives each key to exactly
one slot, so a 10-key stage busies 10 slots and the rest idle regardless of
queue depth.

| Stage | Key | Keys | Slots usable of 20 |
|---|---|---|---|
| dedup | `trade_id` | millions | 20/20 |
| position by account+ticker | `account\|ticker` | 50 | 20/20 |
| price conflation — **salted** | `symbol\|salt` | **80** | **20/20** |
| price conflation — unsalted | `symbol` | 10 | 10/20 |
| mv by account+ticker | `ticker` | 10 | **10/20** |
| mv by ticker | `ticker` | 10 | **10/20** |

**The market-value stages run at 10/20 in every configuration.** Salting cannot
widen them; what it widens is the conflation stage on the raw feed.

**Threading.** At `ParallelismPerKPU=4`, 20 slots sit on 5 vCPUs, and each slot
carries one thread per unchained task — roughly 10, including the exactly-once
committers. That is ~40 threads per core. Idle slots park rather than burn CPU,
so the 63–76% CPU reading is contention and framework overhead, not operator
work: operator busy-time averages ~1% while the busiest subtask hits 100%.

**Who pays for idle capacity:** AWS bills 20 provisioned slots whether or not
ten can be assigned keys. Confluent bills CFU-minutes consumed — the cap-20 pool
simply declined to draw compute it could not use.

---

## What changed

Earlier numbers in this repository were measured against ceilings built into the
test rig. They are retired, not merely dated.

| Retired claim | Why | Now |
|---|---|---|
| AWS 435k rec/s | predates exactly-once and CR-1; different parallelism | 146,300 rec/s |
| "Confluent saturates: +8% for +60% compute" | measured at 6 buckets — added CFUs had nothing to read | right conclusion, wrong reason: the *workload* caps at ~10-way |
| "SQL is 2.69× slower" | that SQL run was stalled and wrote nothing | ~1.9×, output verified |
| "Confluent scales 1.50× for 2× CFUs" | compared runs with different backlogs; ramp-up penalises the smaller | no scaling — the pool never exceeded 10 CFU |
| AWS ~$1.09/hr | omitted the MSK Serverless base charge | $1.84/hr |

**Method rules that produced these corrections**, worth keeping:

* **Verify output, not just input.** Cells were originally validated on records
  consumed, backpressure, restarts and checkpoints — and never on records
  *written*. A pipeline that reads fast and writes nothing scores beautifully.
* **Compare rates, not drain times**, unless backlogs are identical: average
  drain rate includes ramp-up, which penalises smaller backlogs.
* **Audit utilisation before trusting a number**
  (`scripts/utilization_audit.py`): partitions ≥ parallelism, key cardinality ≥
  parallelism per stage.
* **Size backlogs against the drain rate**, not the clock:
  `seed > rate × (blind_spot + plateau)`.
* **Capture metrics during the run** where teardown destroys them.

Raw artefacts: [`docs/evidence/phase16/`](evidence/phase16/) — the CFU
measurements, the CloudWatch export, and a complete drain log reporting
`0.0 records/sec` from a job that looked healthy on every other signal.
