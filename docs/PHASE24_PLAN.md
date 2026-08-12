# Phase 24 — positions only: strip prices, measure the floor

**Goal:** remove the price stream and the market-value outputs entirely. Emit
only `position/symbol` and `position/account+symbol`, and measure what that
costs.

This isolates the trade path. Everything measured so far has been a pipeline
carrying two streams, a join, and four sinks; a large share of the work
(price ingest, the current-price reduce, two joins, two MV sinks) belongs to
the price side. This establishes the floor.

---

## Pipeline under test

| | |
|---|---|
| Inputs | trades only — prices not consumed |
| Symbols | 100 unique |
| Outputs | `position-by-account-ticker`, `position-by-ticker` |
| Removed | price source, current-price reduce, both joins, both MV sinks |
| Dedup | none — unique publisher, exactly-once |
| Partitions | 2, then 4 |

## What this answers

1. **What does the price path actually cost?** Compare against the full
   pipeline at the same trade rate: same trades in, positions out, minus
   everything price-related.
2. **Where is the floor?** Position-only is the simplest correct topology. If
   it still saturates at ~4,000 rec/s the constraint is not the price side.
3. **Does the earlier bottleneck survive?** The trace named
   `SinkUpsertMaterializer` on the MV path. Remove the MV path and it should
   disappear — if throughput does not improve, that operator was not the limit
   after all.

## Runs

| # | engine | partitions |
|---|---|---|
| 1 | SQL | 2 |
| 2 | SQL | 4 |
| 3 | DataStream | 2 |
| 4 | DataStream | 4 |

Both engines, per the standing rule -- three conclusions in this project flipped
once the second engine was measured.

## Checks — every run

Per [RUN_CHECKLIST.md](RUN_CHECKLIST.md). Full status columns (the MV columns
will read 0 by design, not by defect), plus:

* completeness: `sum(accounts) == symbol total`
* correctness: positions equal trade counts
* ordering: `as_of` never backwards, no consumer left on a superseded value
* uniqueness: no duplicates from the source

Market-value checks are not applicable and will be reported as N/A rather than
silently passing.

## Pre-declared interpretation

* **Throughput rises materially** -> the price path was the cost, and the
  earlier bottleneck analysis holds.
* **Throughput unchanged (~4,000 rec/s)** -> the constraint is in the trade
  path or the environment, and the entire materializer investigation was
  chasing a symptom.
* **Utilization stays ~31%** -> whatever pins that number is independent of
  topology, which would be the most interesting result of the three.

## Method rules

* Local only.
* Both engines.
* One run recorded and committed before the next.
* Rates as counter deltas; job must still be consuming at sample end.
* Backpressure from the per-subtask metric, never the `/backpressure` endpoint.
