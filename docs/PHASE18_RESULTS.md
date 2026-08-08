# Phase 18 results — correctness first, then what the numbers actually say

**This supersedes the Phase 16 and 17 performance figures.** Not because they
were sloppily taken, but because they were taken on a workload with **at most 30
symbols** — a `Math.min(requested, 30)` clamp silently capped every run in this
project until it was found here. Several published conclusions were properties
of that benchmark rather than of Flink, Confluent, or the pipeline.

---

## Correctness — established first, and the point of the phase

Both passes green on the production config: 3,000 symbols, Zipf α=1.0, one IPO
symbol at 30% of the tape, adaptive write-time keys, `idle-timeout` set.

| Pass | Config | Result |
|---|---|---|
| **1. Simple numbers** | qty=1, symbol *i* priced $(i+1), static | **six checks pass** |
| **2. Realistic** | random ±qty, walking prices | **six checks pass, 0 wrong** |

```
[PASS] dedup:               509 duplicates, 9,891 unique of 10,400
[PASS] positions acct+tkr:  7,246 keys, 0 mismatched
[PASS] positions by ticker: 2,910 keys, 0 mismatched
[PASS] completeness:        2,910 rolled up, 0 disagree
[PASS] MV by account:       7,155 checked, 0 wrong
[PASS] MV by ticker:        2,876 checked, 0 wrong
```

Each check independently recomputes from the raw topics with exact decimal
arithmetic. Market values are asserted against **the final raw price and our own
recomputed position**, never the pipeline's own conflated topic — checking
against that would be circular, and using the published position would let a
wrong position cancel a wrong price.

**This clears two changes that had shipped on argument rather than evidence:**
`table.exec.source.idle-timeout` is not dropping late ticks, and salted
write-time keys are not shifting window membership.

### Disclosed: conflation lag, ~6% of market values

Pass 2 initially **failed** with 628 mismatched market values. Rather than
attribute them to known tumbling-window semantics, the validator was made to
prove it: the implied price (published MV ÷ recomputed position) must fall
inside the min–max range that symbol actually traded at. **All 615 did** — a
real tick of the right symbol, just not the newest.

So ~6% of market values are priced at the last closed conflation window rather
than the newest tick, because a SQL tumbling window closes on watermark advance.
Staleness is bounded by the 250 ms window. That is the CR-1 design intent, not a
defect — but it is a real product characteristic and it is reported as a WARN,
never folded into a pass.

### Disclosed: MV coverage

~1.3% of positions have no market value yet, because a position exists as soon
as a trade lands while its MV also needs a price for that symbol. Those keys are
**skipped** by the MV checks, and skipped is not correct, so the validator says
so.

---

## Scaling — a single statement caps at ~10 CFU

Identical 13.86M backlog, same topic, no re-seed. Only the pool cap differs.

| Pool cap | Drain | Rate | CFU avg | **CFU max** |
|---|---|---|---|---|
| 10 | 674s | 20,564 rec/s | 8.8 | **10.0** |
| 20 | 1,222s | 11,342 rec/s | 9.05 | **10.0** |

**A single Confluent statement will not draw more than ~10 CFU whatever the pool
allows.** Doubling the cap changed nothing — and this time ~3,000 ticker keys
and ~7,600 account+ticker keys were available, so key starvation is ruled out.

**This vindicates the original Phase 16 finding** that the CFU dial saturates —
a claim overturned once, then re-overturned, both times wrongly. Establishing it
required a key space wide enough to eliminate the alternative explanation, which
the 30-symbol clamp had made impossible.

**What cardinality does change:** at ≤30 symbols the pool drew only 4–6 of an
allowed 10; at 3,000 it draws the full 10. **Cardinality governs how much of the
ceiling is usable, not where the ceiling is.**

**Consequence: scale OUT, not up.** More statements over disjoint key ranges,
each with its own ~10 CFU ceiling. Raising `max_cfu` on one pool buys nothing.

**Not claimed:** cap-20 drained 1.8× slower on identical work at the same CFU
draw. That is outside the ~25% variance band with no supported explanation —
cold pool, or the drain-completion detector lagging on a 5-minute metric window.
It is **not** evidence of negative scaling and is not presented as a result.

---

## The IPO hotspot — still the strongest result, and unaffected

From Phase 17, measured at the producer, and independent of key cardinality
because one partition is one partition at any width:

| Feed | Producer key | Ingest | Ordering |
|---|---|---|---|
| Uniform | `symbol` | 873,333/s | kept |
| **90% one ticker** | `symbol` | **293,333/s** | kept |
| 90% one ticker | `salted` | 764,444/s | lost |
| **90% one ticker** | **`adaptive`** | **788,888/s** | **kept for cold names** |

A hot listing costs **two thirds of ingest** before Flink sees a record: every
producer writing that symbol queues behind the single leader broker owning its
partition, and adding producers makes it worse. **Adaptive keying — salting only
symbols above a share threshold — recovers it to −10%** while quiet names keep
per-symbol ordering and compaction.

Keying fixes **ingest only**. Clean runs showed no processing gain (23,807 vs
23,603 rec/s on identical purged backlogs); spreading the write cannot widen a
stage whose key space is the business domain.

**Surviving an IPO needs both:** spread at the key, and conflate before the
narrow stage. Neither alone suffices.

---

## Cost, measured

**AWS: $57.49 total** across the project (Aug 5–7), of which **64% is Kafka, not
Flink** — MSK Serverless base plus partition-hours. Every pipeline tuning change
in this project moved the smaller half of the invoice.

**Confluent: $79.96 gross, $0.00 net** — fully covered by promotional credit.
The gross split is instructive and matches AWS's shape:

| Line | Gross |
|---|---|
| KAFKA_NUM_CKUS | $42.93 |
| FLINK_NUM_CFUS | $23.29 |
| KAFKA_NETWORK_READ | $8.35 |
| KAFKA_NETWORK_WRITE | $5.31 |

**On both clouds the streaming platform costs more than the stream processing.**

---

## What changed, and why

| Retired claim | Cause | Now |
|---|---|---|
| Every Phase 16/17 throughput figure | measured at ≤30 symbols (silent clamp) | re-measured at 3,000 |
| "Confluent scales 1.50× for 2× CFUs" | compared different backlog sizes; ramp-up bias | no scaling — statement caps at ~10 CFU |
| "The ~10 CFU ceiling is key starvation" | ceiling persists at 3,000 keys | it is a per-statement limit |
| AWS ~$1.09/hr | omitted MSK base charge | $1.84/hr |

**Method rules that produced these corrections**, all now enforced in the
harness:

* **Correctness gates performance, at the volume being claimed.** Phases 16–17
  produced dozens of numbers with the validator never once run successfully.
* **Verify the output side.** A pipeline that reads fast and writes nothing
  scores beautifully on a throughput metric.
* **Compare rates, not drain times**, unless backlogs are identical.
* **Purge tables between runs** — leftover backlogs silently contaminate.
* `sent_records` is **not** records-processed.
* **Sample CFU during the run** — Confluent telemetry dies with the pool.
* **~25% run-to-run variance**: a gap under that is not a result.
* **When two conditions that should differ return the same number, suspect the
  harness.** It caught a rate limiter, and later a 30-symbol clamp.

Evidence: [`docs/evidence/phase18/`](evidence/phase18/).

---

## Latency — and why this number is not comparable to earlier ones

`mv-by-account-ticker`, live feed, statement started at **latest-offset** so this
is latency rather than backlog age:

| n | p50 | p90 | p99 | min |
|---|---|---|---|---|
| 34,206 | **4,247 ms** | 10,342 ms | 45,175 ms | 1,218 ms |

**Read with the arrival regime, or not at all.** At 3,000 symbols on a
500 prices/s feed, each symbol receives a tick roughly every 6 seconds. The
250 ms tumbling window cannot close until a watermark advances past it, so this
figure is dominated by **waiting for the next tick of that symbol**, not by
processing time.

It is therefore **not** comparable to the Phase 12 measurement (p50 1.8 s),
which was taken at 10 symbols and 10,000 prices/s — every symbol receiving
~1,000 ticks/sec. Same pipeline, different arrival rate per key, and per-key
arrival rate is what drives event-time window latency.

**The honest statement:** for a wide, thinly-traded universe, end-to-end latency
is governed by how often each individual symbol trades — not by the platform.
A benchmark that reports a single latency number without stating ticks-per-key
is reporting its own feed shape.
