# Phase 23 — 100 symbols, 2 → 4 partitions, live vs backlog

**Local only.** Four runs, one variable pair: partition count and whether the
producer runs concurrently with the consumer.

---

## Pipeline under test

| | |
|---|---|
| Symbols | 100 unique |
| Source partitions | **2**, then **4** |
| Price handling | **`conflate=0`** — no input window; always holds the newest tick |
| Output rate | **`emit=1000`** (positions 500) — publish at most 1/sec per key |
| Outputs | `position/account/symbol`, `position/symbol`, then join prices → `mv/account/symbol`, `mv/symbol` |

Four outputs, two key shapes, market values on the same two keys as positions.

## The four runs

| # | partitions | mode | what it isolates |
|---|---|---|---|
| 1 | 2 | **live** — producer and consumer together | steady-state behaviour |
| 2 | 2 | **backlog** — queue all data first, then consume | capacity, no producer CPU competition |
| 3 | 4 | live | partition effect, steady state |
| 4 | 4 | backlog | partition effect, capacity |

Live vs backlog matters because they measure different things: live shows what
the pipeline does when fed in real time; backlog drain shows what it *can* do.
Phase 21 showed local live runs are producer-bound, so both are needed.

## Checks — every run, every output

1. **Completeness** — every input trade accounted for; `sum(accounts) == symbol total`
2. **Correctness** — positions equal deduped trade counts; market value equals
   position × price
3. **No out-of-sequence publishing** — per key, `as_of` never goes backwards,
   no older price used after a newer one, no consumer left on a superseded value
4. **Staleness** — p50/p90/p99/max, in ms

## Status output

One table per run, standard format:

| Condition | in trades/s | in prices/s | out positions/s | out MV/s | Parallelism | Utilization % | Total $/hr | Flink KPU | Flink $/hr | BackPressure | Kafka partitions | Kafka $/hr |

Plus correctness/ordering/staleness alongside.

## Why not the 250ms tumbling window

The original plan specified a 250ms tumbling window on prices. Dropped, because
it is strictly worse on both axes — measured in Phase 21 on identical input:

| | records published | staleness | exact |
|---|---|---|---|
| 250ms tumbling window | 55,389 | 2,929ms | 0% |
| **`conflate=0` + `emit=1000`** | **18,386** | **0ms** | **100%** |

It costs 3x MORE writes AND returns stale values. There is no trade being made.

The reason is structural: a tumbling window reduces the **input**, discarding the
newest tick before anything uses it. An emit interval reduces the **output**,
publishing the newest value less often. Same volume goal; only one of them throws
away the answer.

Consequence for this phase: the correctness checks can now actually pass, so a
failure means something. On the windowed config every run would have reported the
same expected failure and the 2->4 partition signal would have been harder to read.

If the window's behaviour across partition counts is wanted, add it as runs 5-6
rather than putting a known defect under all four.

**Caveat on that recommendation:** those figures come from 100 symbols at
2,000 prices/s with 4 partitions. The window has not been tested at 2 partitions
specifically. The mechanism is about which tick survives rather than how many
readers there are, so partition count should not change it — but that is
reasoning, not measurement.

## Open question carried in

Salt is a parallelism device and should have nothing to do with correctness, yet
DataStream at `price.salt.factor=1` published 490 of 500 keys with an
out-of-order price while salt=8 published none. The event-time guard in the
market-value operators should have prevented that either way. **Unexplained** —
worth resolving inside this phase since run 1 exercises exactly that path.

## Method rules

* Local first; no cloud in this phase.
* One run, recorded and committed, before the next.
* Verify launches by log content, never a process count.
* Report what fails as prominently as what passes.
