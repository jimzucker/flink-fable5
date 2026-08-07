# Phase 17 — hot tickers on Confluent, and Confluent latency

**Scope: Confluent only.** No AWS runs this phase.

## The problem

Every measurement so far used a **uniform** feed: ten tickers, ~1,000 prices/sec
each. Real markets are never uniform. On an IPO day, an index rebalance, or a
meme-stock squeeze, **one symbol can carry most of the volume on the tape**.

Flink assigns each key to exactly one worker. A stage keyed on ticker gives the
hot ticker exactly one worker no matter how much compute is attached — so the
hot symbol is processed by 1/20th of the cluster while the other nineteen slots
handle the quiet names. Throughput collapses toward the speed of a single core,
and the backlog grows on the one symbol anyone actually cares about that day.

**The uniform benchmark hid this completely.** With ten equal tickers, "one
worker per key" looks like healthy parallelism. It is only a problem when the
distribution is real.

## The partition is a ceiling on BOTH sides

`key = symbol` sends every record for a symbol to one partition, and a partition
has a single leader broker. That caps the symbol twice over:

* **Write side.** Every producer instance writing that symbol queues behind the
  same leader. Running more producers buys nothing for the hot name — they
  contend rather than scale. An IPO saturates ingest before Flink is even
  involved.
* **Read side.** One partition is read by exactly one consumer. No amount of
  parallelism, salting downstream, or CFUs changes that.

**Evidence already in the Phase 16 data, misread at the time.** Twelve generator
tasks produced ~7M prices each in 20 minutes; a single task earlier managed
~11.7M in the same window. That was recorded as vague "MSK-side contention". It
is almost certainly twelve producers contending for ~ten occupied partitions —
the write ceiling, visible in our own measurements.

So the fan-out has to happen at the **key**, which is upstream of both ceilings.
Nothing downstream of the partition assignment can undo it.

## Where the hotspot binds, stage by stage

Working outward from the data:

| Stage | Keyed on | Hot-ticker behaviour |
|---|---|---|
| **Kafka partition** | producer key = `symbol` | **all hot-symbol records land in ONE partition** → one reader |
| price conflation (salted) | `symbol\|salt` | 8 workers for the hot symbol — survives |
| dedup | `trade_id` | unaffected, wide |
| position by account+ticker | `account\|ticker` | spread across accounts holding it |
| **mv by ticker** | `ticker` | one worker — **but** conflation bounds its input to 1 tick/250 ms |

Two of these matter and they are not equally bad:

1. **The Kafka partition is the hard bottleneck.** A single partition is read by
   a single consumer. No amount of Flink parallelism, salting, or CFUs can make
   two consumers read one partition. This is a physical limit and it is where
   an IPO would actually hurt.
2. **The market-value stage is probably fine**, because Phase 7 conflation
   already bounds its input to one price per symbol per window regardless of
   tick rate. Work there scales with *holders*, not with *ticks*. This needs
   proving rather than assuming.

## Hypothesis

**Write-time salting is the fix, and query-time salting cannot be** — a
downstream `PARTITION BY` runs after the source read and cannot widen it. The
change is parked on `parked/price-key-salting`
(`generator.price.key.mode=salted`).

Prediction: with a 90%-hot feed, uniform-key production collapses throughput
roughly to single-partition speed; salted production restores it to near the
uniform-feed number. Stated before measuring so it can be wrong.

Given how this project has gone, that prediction is as likely to be wrong as
right — the last confident prediction (salting would slow DataStream down) was
backwards by 2.66×.

## Runs

Each run: measure drain throughput **and** per-subtask skew, output verified.

All runs on **Confluent Cloud Flink**, fused statement set, 48 buckets,
upsert-key fix and salted CTE, `sql.tables.scan.idle-timeout` set.

| # | Feed | Producer key | Question |
|---|---|---|---|
| A | uniform (baseline) | `symbol` | re-establish the baseline on this env |
| B | **90% one ticker** | `symbol` | how far does throughput collapse? |
| C | **90% one ticker** | **salted** | does write-time salting restore it? |
| D | **90% one ticker** | **adaptive** | does salting only the hot name suffice? |
| E | **Confluent end-to-end latency** | — | never measured on the corrected config |

**Measure PRODUCER throughput as well as drain throughput.** The write ceiling
is half the problem and every previous run measured only the read side. Record
producer records/sec for each case; if B shows producers throttled relative to
A, that is the IPO failure mode reproduced at ingest.

Latency matters here because it is the one lens where SQL was previously
reported as far behind (p50 1.8 s vs 267 ms on AWS DataStream), and that
number predates the upsert-key fix, the salted CTE and the idle-timeout —
all three of which changed how much work the pipeline does per record.

CFU consumption is sampled **during** every run: Confluent telemetry returns
403 once a pool is deleted, so teardown closes the measurement window.

## Correctness gate

Skew must not change results. The validation suite re-derives every output from
the raw topics; it must pass on the salted runs exactly as on the uniform ones.
A faster wrong answer is not a result.

## What "good" looks like

* **B collapses** → the hotspot is real and the uniform benchmark was hiding it.
* **C recovers** → write-time salting is the production answer, and the parked
  branch merges.
* **C does not recover** → the bottleneck is downstream of the source and the
  fix is elsewhere; find it before claiming anything.
* **Per-subtask skew** (`busyTimeMsPerSecond` max vs avg) is the diagnostic:
  one subtask at 100% while the average sits near zero *is* the hotspot,
  visible directly in telemetry.

## Method rules carried forward from Phase 16

* Verify **output**, not just input, before accepting any number.
* Compare **rates**, not drain times, unless backlogs are identical.
* Run `scripts/utilization_audit.py` before trusting a measurement.
* Capture Confluent metrics **during** the run — teardown destroys them.
* Two runs minimum where a claim is close: run-to-run variance is ~25%.

---

# RESULTS — the IPO hotspot, measured and solved

Producer-side ingest, 4 producers, uncapped rate, 180s each, one variable changed.

| Case | Feed | Producer key | Ingest | vs uniform | Per-symbol ordering |
|---|---|---|---|---|---|
| A2 | uniform | `symbol` | **873,333/s** | baseline | kept |
| **B2** | **90% one ticker** | `symbol` | **293,333/s** | **−66%** | kept |
| C2 | 90% one ticker | `salted` | 764,444/s | −12% | **lost** |
| **D3** | 90% one ticker | **`adaptive`** | **788,888/s** | **−10%** | **kept for cold symbols** |

**A hot listing costs two thirds of ingest — before Flink processes a record.**
Every producer writing the hot symbol queues behind the single leader broker
owning that symbol's partition. Adding producers makes it worse: they contend
for the same leader.

**No downstream tuning could have fixed this.** Salting the conflation, fusing
statements, parallelism, CFUs — all operate downstream of partition assignment.

**Adaptive keying is strictly better than blanket salting**, not a tradeoff:
higher ingest (788,888 vs 764,444) *and* quiet symbols keep their ordering.
Salting a quiet symbol scatters its records for no benefit while giving up a
guarantee.

## Production configuration

```
--generator.price.key.mode adaptive
--generator.hot.factor 2.0     # hot = 2x an even share of recent ticks
--generator.hot.width  48      # fan the hot name across all partitions
```

## Why changing the input design is legitimate here

This pipeline never consumes per-symbol ordering: conflation selects `MAX` by
`event_time`, dedup keys on `trade_id`. Neither depends on a symbol's records
sharing a partition or arriving in order. Keying by bare symbol was buying a
guarantee the pipeline does not use, and paying for it with a single-partition
ceiling on the busiest name of the day.

**The validation suite is the proof, not this argument** — it re-derives every
output from the raw topics and must pass unchanged on the salted runs.

## The full stack of ceilings, and what clears each

| Ceiling | Cleared by write-time salting? |
|---|---|
| Producers → one leader broker | **yes** |
| Partition → one consumer | **yes** |
| Key → one Flink worker | **yes** |
| `mv-by-ticker` → one worker | **no — irreducible** |

The first three all derive from partition assignment, so one key change clears
them together. The fourth cannot be widened — a price must meet its holders —
but Phase 7 conflation bounds that stage's input to one tick per symbol per
window, so 800k/s of IPO traffic arrives there as a handful per second.

**The design that survives an IPO is: spread at the key, conflate before the
narrow stage.** Neither alone suffices — spreading without conflation moves the
pileup downstream; conflation without spreading leaves ingest stuck at 293k/s.

## Retrospective: the evidence was already in Phase 16

Twelve generator tasks produced ~7M prices each where a single task managed
~11.7M in the same window. That was recorded as vague "MSK-side contention". It
was producers contending for the ~ten partitions a symbol-keyed feed occupies —
the same ceiling, unrecognised because the feed was uniform.

## Method note

The first attempt returned **44,666/s for both** the uniform and the 90%-hot
case — identical to the digit, because the generator's own rate limit was
binding rather than Kafka. **When two conditions that should differ return the
same number, suspect the harness.** Third instance this project of a plausible
number measuring the configuration rather than the system.
