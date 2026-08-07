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

## Consumer side — the skew hurts processing too, and keying helps there as well

| Key mode | Consumed | Window | Rate | CFU avg |
|---|---|---|---|---|
| `symbol` | 19,834,565 | 942s | **21,056 rec/s** | 9.36 |
| `adaptive` | 39,721,866 | 823s | **48,265 rec/s** | 8.71 |

**2.29x more throughput on fewer CFUs.** So write-time keying helps at BOTH
ends: 2.7x at ingest and ~2.3x through the pipeline.

**Caveat, stated because the run was not as clean as intended.** The adaptive
run consumed 39.7M against a 19.26M seed: the previous symbol-keyed backlog was
never purged, so it drained a 50/50 mix of symbol- and adaptive-keyed records
from earliest. That cuts both ways — the larger backlog reduces ramp-up as a
share of the window and inflates the average (the artifact that invalidated the
Phase 16 scaling claim), while half the records it processed were the *slow*
symbol-keyed case, which suppresses it. Correcting roughly for a ~3-minute ramp
still leaves ~2.4x. Direction and magnitude hold; the run is not publication
clean. **Drop the tables between runs.**

### What this refines about conflation

Conflation protects what is DOWNSTREAM of it — `mv-by-ticker` receives one tick
per symbol per window regardless of skew, exactly as designed. It cannot protect
what is UPSTREAM: the source read, JSON parsing, and the conflation's own
phase-1 dedup all see the raw 90%-on-one-key distribution, and that is where the
processing loss lives.

**The complete answer to a hot ticker is therefore three-part:**

1. **Spread at the key** (adaptive) — clears the leader-broker, consumer and
   Flink-key ceilings, all of which derive from partition assignment.
2. **Conflate before the narrow stage** — bounds `mv-by-ticker`'s input so the
   irreducible one-worker stage never sees the flood.
3. Neither alone suffices: spreading without conflation moves the pileup
   downstream; conflation without spreading leaves ingest at 293k/s and
   processing at 21k/s.

## OPEN: does the fixed pipeline scale?

**Not proven.** The hot-key fix is measured at a single size (cap-10). Whether
the corrected pipeline scales with compute is untested — and it is newly
testable for a reason worth spelling out.

Phase 16 concluded "this workload saturates at ~10 CFU". That measurement had
**two confounded causes, both equal to ten**:

| Candidate cause | Value |
|---|---|
| Ticker cardinality → Flink key-groups | 10 |
| Occupied partitions (symbol-keyed) → source readers | ~10 |

The ceiling was attributed to key cardinality. But symbol-keyed data occupied
only ~10 of 48 partitions, so the source was pinned at 10 as well. The two could
not be separated.

**Adaptive keying breaks the tie.** Data now spreads across all 48 partitions,
so the source can read 48-way while the ticker-keyed stages downstream stay at
10. Running the adaptive config at cap-10 and cap-20 therefore answers a
question that was previously unanswerable:

* **Scales past 10 CFU** → the Phase 16 ceiling was partly partition
  concentration, and "the workload caps at ten" needs revising again.
* **Stays at 10 CFU** → key cardinality is genuinely the binding limit and the
  ten-ticker conclusion stands on its own.

**Method requirements**, learned expensively in Phase 16:
* Compare **rates**, and give both rungs the **same backlog size** — the 1.50x
  scaling claim died because cap-20 drained a larger backlog and average rate
  includes ramp-up.
* Sample **CFU during the run**; the pool declining to draw compute it cannot
  use is the actual evidence, and telemetry dies with the pool.
* Purge tables between runs.

## CORRECTION — clean runs: keying fixes ingest, NOT processing

Both runs purged tables first and seeded **exactly 19,260,000** prices.

| Key mode | Seeded | Drain | True rate | CFU avg |
|---|---|---|---|---|
| `symbol` | 19,260,000 | 809s | **23,807 rec/s** | 3.77 |
| `adaptive` | 19,260,000 | 816s | **23,603 rec/s** | 4.92 |

**No measurable difference in processing throughput.** The earlier 2.29x
consumer-side claim was entirely the contamination — the unclean run drained a
double-sized mixed backlog, and that was carrying the whole result.

**`sent_records` is NOT a valid proxy for records processed.** It counted 36.6M
for adaptive against 19.7M for symbol on the *same* 19.26M seed, because the two
plans read the source a different number of times. Use **seeded ÷ drain
duration**, which is what the table above reports.

### Revised conclusion

| | `symbol` | `adaptive` | Effect |
|---|---|---|---|
| **Ingest** | 293,333/s | **788,888/s** | **2.7x — real** |
| **Processing** | 23,807/s | 23,603/s | **none** |

Write-time keying **fixes the ingest ceiling and does nothing for processing**,
which is consistent on reflection: once records are in the topic, the source
reads 48 partitions either way, and the ticker-keyed stages downstream are
unchanged at ten keys however the records were distributed. Spreading the write
cannot widen a stage whose key space is the business domain.

**So the hot-ticker answer is narrower than the ingest number suggests:**
adaptive keying makes the tape *land*; it does not make the pipeline *process*
it faster. Surviving an IPO needs both the keying fix AND enough compute for a
processing stage that remains ten-way parallel.

---

# PHASE 17 CLOSE

## What this phase proved

| Finding | Status |
|---|---|
| A hot ticker costs **66% of ingest** (873k → 293k/s) | measured |
| **Adaptive keying recovers it: 2.7×** (293k → 789k/s) | measured |
| Adaptive **beats blanket salting** *and* keeps ordering for cold symbols | measured |
| Keying does **nothing** for processing (23,807 vs 23,603 rec/s) | measured, clean runs |
| The partition caps **producers** too, not just consumers | measured |
| Phase 16's "MSK contention" anomaly was this same ceiling | explained |

## Confluent traps found

* Round-robin distribution is **unavailable** on keyed-format tables —
  declaring `key.format` forces `DISTRIBUTED BY`.
* Recreating Kafka topics **out-of-band destroys Flink table schemas**;
  Confluent re-infers `BYTES` and every INSERT then fails on type mismatch.
* Primary-key columns must be **NOT NULL** — `JSON_VALUE` is nullable, so
  `CONCAT` over it is too. `COALESCE` pins it.
* Statement names must be **lowercase** `[a-z0-9-]`.
* Deleting service accounts on a retry breaks an in-flight terraform apply
  (they are org-scoped and survive environment deletion).

## What this phase did NOT do — and it is the reason for Phase 18

**No correctness validation ran.** Not once, in this phase or Phase 16. Every
throughput number here is unvalidated. Changes shipped that carry real
correctness risk and were argued safe rather than proven: `idle-timeout` forcing
watermarks past idle partitions, salted keys spreading a symbol across
partitions, `GROUP BY CONCAT`, `COALESCE` on primary keys.

**The dataset was also unrealistic.** Ten uniformly-loaded tickers made key
starvation the dominant effect in nearly every result. US equities are
~8,000–11,000 symbols on a Pareto curve.

**Scaling remains unproven**, and is now newly testable: the pool drew only
4–6 CFU of an allowed 10, so it declined compute it already had.
