# Phase 22 — like-for-like: local vs AWS vs Confluent

**Goal: run the SAME configuration, the SAME feed, and the SAME measurements in
all three environments, and put them in one table.**

Every cross-environment number this project has published so far was measured a
different way in each place, which is what forced the retractions. This phase
fixes the method first and measures second.

---

## What "like-for-like" requires

| dimension | must be identical | notes |
|---|---|---|
| Engine config | SQL: `conflate=0`, `emit=1000`<br>DataStream: `salt.factor=8`, `emit=1000` | the working configs from Phase 21 |
| Feed | 200 symbols, 2,000 prices/s, 200 trades/s, adaptive keying, 30% IPO | simple numbers so correctness is checkable |
| Partitions | 16 everywhere | local is currently 4 — **must be raised** |
| Parallelism | 8 everywhere | local is currently capped at 4 slots — **must be raised** |
| Measurements | six checks, ordering checks, staleness percentiles, in/out rates | one validator, one status table |

## Known blockers, to resolve BEFORE measuring

1. **Local is 4 partitions / 4 slots.** Raise to 16 / 8 or the parallelism
   column cannot match.
2. **Local Kafka is source-bound at ~5,000-6,000 rec/s** (measured: 31% busy,
   0% backpressure at both P=2 and P=4 during a drain). So **throughput is NOT
   comparable from local** — only correctness and ordering are. State this in
   the table rather than letting the numbers imply otherwise.
3. **Confluent cannot yet run the working config.** Terraform deploys the `dml/`
   files individually and never references a statement set, so
   `confluent/sql/working/statement_set.sql` needs a new resource.
4. **Unverified: whether Confluent exposes an output reduce.** AWS uses the
   upsert-kafka option `sink.buffer-flush.interval`; Confluent uses managed
   tables (`changelog.mode`, `key.format`, `value.format`). Test with a CREATE
   TABLE before assuming. If it is absent, Confluent cannot run the working
   config at all — which is a capability finding, not a performance one.

## What is comparable, and what is not

| | local | AWS | Confluent |
|---|---|---|---|
| Correctness (six checks) | ✓ | ✓ | ✓ |
| Ordering (as_of / price / ending stale) | ✓ | ✓ | ✓ |
| Staleness percentiles | ✓ | ✓ | ✓ |
| Output volume (records published) | ✓ | ✓ | ✓ |
| **Throughput / capacity** | **✗ source-bound** | ✓ | ✓ |
| Cost | n/a ($0) | ✓ | ✓ |

Being explicit about the ✗ is the point. Local throughput measures the laptop's
Kafka, not the pipeline.

## Steps

1. **Free, local:** raise local partitions to 16 and slots to 8; change
   `price.salt.factor` default 1 -> 8; re-verify both engines.
2. **AWS (~$3):** both working configs, correctness + ordering + volume.
   *(In flight at phase start.)*
3. **AWS (~$4):** throughput on the working SQL config, against the 6,106 rec/s
   on record for the broken one.
4. **Confluent:** resolve blockers 3 and 4 first, then the same run.

## Pre-declared interpretation

* **SQL working config materially beats 6,106 rec/s** -> the published
  DataStream-vs-SQL gap is overstated and gets restated.
* **Confluent cannot express the output reduce** -> report as a platform
  capability difference; do not substitute a throughput number for it.
* **Any environment differs on correctness or ordering** -> that is the headline,
  ahead of any speed comparison.

## Method rules carried forward

* Test BOTH engines — three conclusions changed in Phase 21 once the second was.
* Local first for code; cloud only for what needs real Kafka.
* One condition, recorded, committed, then the next.
* Verify launches by log CONTENT, never a process count or exit code.
* Unconditional teardown plus an independent watchdog.
